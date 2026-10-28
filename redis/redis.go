package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

const (
	taskKeyPattern = "{timertask.%s}.shard.%d"
	heartbeatKey   = "{timertask.%s}.heartbeat"
	deadLetterKey  = "{timertask.%s}.dead"
	// defaultBatch 单次扫描拉取的任务条数上限。
	defaultBatch = 300
	// deadLetterMaxLen 死信 ZSet 保留上限（按失败时间只保留最近 N 条），防止无限增长。
	deadLetterMaxLen = 10000
)

// bestEffortTimeout 是「尽力而为」写（死信写入、租约 Ack）的超时上限。
//
// 这些调用都发生在**失败/收尾路径**上：业务已经结束，若 Redis 是黑洞
// （网络分区而非拒连），无 deadline 的调用会挂到 OS 级 TCP 超时
// （分钟级）⇒ worker goroutine 永不返回 ⇒ worker 池被拖垮、优雅停机卡住。
// 超窗后按 best-effort 语义放弃（只记日志）。
//
// 用 var 而非 const：测试需要把它调小以在毫秒级内复现「黑洞」场景。
var bestEffortTimeout = 5 * time.Second

// deleteScript Lua 原子删除脚本（原子 claim 消费到的 task）。
//
// 条件: task 实际 score (EndTime) <= ARGV[2] (current scan time) 时删除。
// 之前用 `zscore == ARGV[2]` 是 bug：ARGV[2] 是消费循环的当前秒级时间戳，
// 而 task 的 EndTime 几乎不可能精确相等，导致 deleteScript 永远不返 1，
// task 永远卡在 ZSet 无人消费。
//
// 改为 `<=` 后语义正确：
//   - 当前 scan 拉出的 task，其 EndTime 必然 <= now
//   - 原子删除保证多 worker 并发下只有一方 claim 成功
//
// ⚠️ 必须显式 tonumber（2026-09-06 复审修复）：Lua 5.1 对两个 string 的
// `<=` 是字典序比较，zscore 返回 bulk string、ARGV[2] 也是 string —
// 等长（10 位 epoch）时恰好等价数值比较，但 score 位长不同（如 9 位的
// EndTime=999999999，数值上已到期）时条件恒假:任务永远无法 claim 且
// ZRangeByScore 按 score 升序会让卡死任务占满批次饿死整个 shard。
var deleteScript = goredis.NewScript(`
	local score = redis.call('zscore', KEYS[1], ARGV[1])
	if score and tonumber(score) <= tonumber(ARGV[2]) then
		return redis.call('zrem', KEYS[1], ARGV[1])
	else
		return 0
	end
`)

var log = logx.Delegating("timertask/redis")

// RedisBackend 是基于 Redis ZSet 的分布式延迟任务实现。
type RedisBackend struct {
	client     goredis.UniversalClient
	buzType    string
	handler    timertask.Handler
	onFailure  func(*timertask.Task, error)
	buckets    int
	workers    int
	retryCount int
	retryDelay time.Duration

	// 分片管理
	shardManager *ShardManager

	// 投递语义：timertask.DeliveryAtMostOnce（默认）或 timertask.DeliveryAtLeastOnce（租约+Ack）
	delivery          timertask.DeliveryMode
	visibilityTimeout time.Duration

	// 本地队列：携带原始 ZSet member，可靠模式 Ack 时使用
	localQueue chan *claim
	stopChan   chan struct{}
	stopOnce   sync.Once
}

// claim 是本地队列中的已 claim 任务，member 为 ZSet 原始成员串。
type claim struct {
	task   *timertask.Task
	member string
}

// NewRedisBackend 创建基于 Redis 的延迟任务后端。
//
//   - buzType: 业务类型标识（区分不同业务的任务）
//   - client: Redis 客户端（支持单机和集群）
//   - handler: 任务处理函数
func NewRedisBackend(buzType string, client goredis.UniversalClient, handler timertask.Handler, opts ...timertask.TimerTaskOption) *RedisBackend {
	cfg := timertask.ResolveOptions(opts...)
	b := &RedisBackend{
		client:            client,
		buzType:           buzType,
		handler:           handler,
		onFailure:         cfg.OnFailure,
		buckets:           cfg.Buckets,
		workers:           cfg.Workers,
		retryCount:        cfg.RetryCount,
		retryDelay:        cfg.RetryDelay,
		delivery:          cfg.Delivery,
		visibilityTimeout: cfg.VisibilityTimeout,
		localQueue:        make(chan *claim, 100000),
		stopChan:          make(chan struct{}),
	}

	// 创建分片管理器
	b.shardManager = NewShardManager(buzType, client, b.buckets)

	return b
}

// Send 提交一个延迟任务。
// task.EndTime（Unix 秒）决定任务何时被执行。
func (b *RedisBackend) Send(ctx context.Context, task *timertask.Task) error {
	if task.ID == "" {
		return timertask.ErrEmptyTaskID
	}
	if task.CreateTime == 0 {
		task.CreateTime = time.Now().Unix()
	}
	if task.MaxRetry == 0 {
		task.MaxRetry = b.retryCount
	}
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("timertask: marshal task: %w", err)
	}

	key := b.taskKey(task.ID)
	_, err = b.client.ZAdd(ctx, key, goredis.Z{
		Score:  float64(task.EndTime),
		Member: string(data),
	}).Result()
	if err != nil {
		return fmt.Errorf("timertask: ZADD %s: %w", key, err)
	}
	log.Info("task scheduled", "taskID", task.ID, "endTime", task.EndTime)
	return nil
}

// Start 启动任务消费（阻塞）。
// ctx 取消时所有 goroutine 退出。
func (b *RedisBackend) Start(ctx context.Context) error {
	// 启动心跳
	async.GoSafe(func() { b.shardManager.StartHeartbeat(ctx) })

	// 启动分片分配
	async.GoSafe(func() { b.shardManager.StartAssign(ctx) })

	// 启动远程队列消费
	async.GoSafe(func() { b.consumeRemoteLoop(ctx) })

	// at-least-once：启动租约回扫——崩溃实例未 Ack 的租约到期后由本循环捡回重投
	if b.delivery == timertask.DeliveryAtLeastOnce {
		async.GoSafe(func() { b.reapLoop(ctx) })
	}

	// 启动本地 Worker 池
	for i := 0; i < b.workers; i++ {
		async.GoSafe(func() { b.workerLoop(ctx) })
	}

	<-ctx.Done()
	// 优雅停机：先 close(stopChan) 让 worker 在任务边界退出，再回灌本地队列。
	// 顺序颠倒时 worker 可在回灌的同时用已取消的 ctx 抢跑任务——handler 立刻
	// 失败、重试全败，从未真正处理的任务被写进死信 ZSet（假死信）。
	// stopOnce 防二次 Start 重复 close panic（与 LocalBackend 对称）。
	b.stopOnce.Do(func() { close(b.stopChan) })
	b.requeuePendingOnShutdown()
	// at-least-once：停机同步补一轮租约回扫（reapLoop 内也会做，这里兜底时序竞态）
	if b.delivery == timertask.DeliveryAtLeastOnce {
		b.requeueLeasesOnShutdown()
	}
	return ctx.Err()
}

// requeuePendingOnShutdown 将本地队列中尚未处理的任务回灌 Redis（ZADD 回原分片，
// 保留原 EndTime），重启后由任意实例继续消费。仅覆盖优雅停机路径；
// 进程崩溃时队列内任务仍会丢失（见包文档的 at-most-once 语义）。
func (b *RedisBackend) requeuePendingOnShutdown() {
	requeued := 0
	// 回灌窗口限时：Redis 黑洞（网络分区而非拒连）时无 deadline 的 ZAdd 会
	// 挂到 OS 级 TCP 超时（分钟级），worker 无法退出、Start 调用方卡死。
	// 超时后剩余任务按 doc 声明的 at-most-once 语义丢弃。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			if requeued > 0 {
				log.Warn("requeue window expired, remaining tasks dropped (at-most-once)",
					"requeued", requeued)
			}
			return
		case c := <-b.localQueue:
			if c == nil || c.task == nil {
				continue
			}
			task := c.task
			data, err := json.Marshal(task)
			if err != nil {
				log.Error("requeue marshal failed", "taskID", task.ID, "err", err)
				continue
			}
			key := b.taskKey(task.ID)
			pipe := b.client.Pipeline()
			pipe.ZAdd(ctx, key, goredis.Z{
				Score:  float64(task.EndTime),
				Member: string(data),
			})
			if b.delivery == timertask.DeliveryAtLeastOnce {
				pipe.ZRem(ctx, b.processingKey(), c.member)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				log.Error("requeue ZAdd failed", "taskID", task.ID, "err", err)
				continue
			}
			requeued++
		default:
			if requeued > 0 {
				log.Info("requeued pending tasks on shutdown", "count", requeued)
			}
			return
		}
	}
}

// consumeRemoteLoop 定期扫描到期任务并推入本地队列。
func (b *RedisBackend) consumeRemoteLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopChan:
			return
		case <-ticker.C:
			b.consumeRemoteOnce(ctx)
		}
	}
}

func (b *RedisBackend) consumeRemoteOnce(ctx context.Context) {
	now := time.Now().Unix()

	// 只扫描当前负责的分片
	shards := b.shardManager.GetMyShards()
	for _, shard := range shards {
		key := b.shardKey(shard)

		// 使用 Lua 脚本原子获取并删除到期任务
		tasks, err := b.client.ZRangeByScore(ctx, key, &goredis.ZRangeBy{
			Min:    "0",
			Max:    strconv.FormatInt(now, 10),
			Offset: 0,
			Count:  defaultBatch,
		}).Result()
		if err != nil {
			if !errors.Is(err, goredis.Nil) {
				log.Error("ZRangeByScore error", "err", err)
			}
			continue
		}

		for _, data := range tasks {
			var ok bool
			var err error
			if b.delivery == timertask.DeliveryAtLeastOnce {
				ok, err = b.claimTaskWithLease(ctx, key, data, now)
			} else {
				var result int
				result, err = deleteScript.Run(ctx, b.client, []string{key}, data, now).Int()
				ok = result > 0
			}
			if err != nil {
				log.Error("claim script error", "err", err)
				continue
			}
			if ok {
				task := b.parseTask(data)
				if task == nil {
					log.Error("failed to parse task from ZSet, data discarded", "key", key)
					continue
				}
				// 推入本地队列
				select {
				case b.localQueue <- &claim{task: task, member: data}:
				case <-ctx.Done():
					return
				case <-b.stopChan:
					return
				}
			}
		}
	}
}

// workerLoop 从本地队列消费任务。
func (b *RedisBackend) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopChan:
			return
		case c := <-b.localQueue:
			if c == nil || c.task == nil {
				continue
			}
			b.processTask(ctx, c)
		}
	}
}

// processTask 处理单个任务，支持重试。可靠模式下成功或终态失败会 Ack 租约。
func (b *RedisBackend) processTask(ctx context.Context, c *claim) {
	task := c.task
	var err error
	normalizeRetry(task)
	// terminal 标记「本次处理已到终态」（成功 / 已进死信）——只有终态才 Ack 租约。
	//
	// 此前 defer 里的 Ack 在**每条**返回路径上都执行，包括「重试窗口内被
	// 中断」（ctx 取消 / 停机）那条——与同处注释「仅 handler 仍在重试窗口内
	// 不 Ack」直接矛盾。后果是任务**永久丢失**：任务已被 claim 出分片 ZSet
	// 移进 processing 租约集，租约是它唯一的去处，释放了它，任务就落到
	// 「哪都不是」的地方（不在分片 ZSet、不在 processing、不在死信 ZSet），
	// 租约回扫（reapExpiredLeases）也捞不回来 ⇒ at-least-once 承诺失效
	// （doc.go 声明的「租约超时由回扫重投」形同虚设）。
	//
	// 中断时**不** Ack：租约留在 processing 里，由 reapExpiredLeases（租约到期）
	// 或 requeueLeasesOnShutdown（停机）捡回重投。
	terminal := false
	defer func() {
		if terminal && b.delivery == timertask.DeliveryAtLeastOnce {
			// 成功或已进死信的终态：释放租约。
			// Ack 带限时：它在**收尾路径**上，Redis 黑洞时无界的调用会把
			// worker 挂住 ⇒ worker 池被拖垮、优雅停机卡住。
			// 超窗按 best-effort 语义放弃。
			bestEffortRedis("ack", func(ctx context.Context) error {
				b.ackTask(ctx, c.member)
				return nil
			})
		}
	}()
	// retryTimer lazy init: i=0 直接调 handler，不需要 timer；只有真正进入重试路径
	// 才创建 timer，避免每次成功任务都起一个未消费的 timer（defer Stop 不会释放
	// 已经被 runtime 调度过但未读取的 tick，长时间运行会累积）。
	// defer 在 if 块里只触发 1 次，gocritic deferInLoop 是误报。
	var retryTimer *time.Timer
	for i := 0; i <= task.MaxRetry; i++ {
		if i > 0 {
			// 重试延迟
			if retryTimer == nil {
				retryTimer = time.NewTimer(b.retryDelay)
				//nolint:gocritic // lazy init：仅首次进入重试路径时分配，defer 与 NewTimer 配对
				defer retryTimer.Stop()
			} else {
				retryTimer.Reset(b.retryDelay)
			}
			select {
			case <-ctx.Done():
				return
			case <-b.stopChan:
				return
			case <-retryTimer.C:
			}
			task.RetryCount = i
			log.Warn("retrying task", "taskID", task.ID, "attempt", i, "maxRetry", task.MaxRetry)
		}

		// handler panic 就地收编为任务失败：panic 穿透 workerLoop 会打死该 worker
		// goroutine（async.GoSafe 的 recover 在出口、不重启），N 个 panic 任务耗尽
		// worker 池后消费瘫痪，且任务已从 ZSet claim、不会进死信 ZSet——永久丢失。
		// 收编后走既有重试 + sendToDeadLetter 路径。
		err = runHandlerSafe(ctx, b.handler, task)
		if err == nil {
			terminal = true
			return
		}
		log.Error("handler error", "taskID", task.ID, "err", err)
	}

	// 重试次数用完：先写入持久死信（best-effort），再触发进程内回调
	log.Error("task failed after retries", "taskID", task.ID, "maxRetry", task.MaxRetry, "err", err)
	b.sendToDeadLetter(task)
	if b.onFailure != nil {
		b.onFailure(task, err)
	}
	// 终态（已进死信）：租约可以释放。
	terminal = true
}

// sendToDeadLetter 将重试耗尽的任务写入持久死信 ZSet（score 为失败时间戳），
// 供人工排查或补偿任务消费。写入为 best-effort：失败仅记录日志，不阻断 onFailure。
// 失败原因已在调用方日志中携带，死信只存任务本体。
func (b *RedisBackend) sendToDeadLetter(task *timertask.Task) {
	data, err := json.Marshal(task)
	if err != nil {
		log.Error("dead letter marshal failed", "taskID", task.ID, "err", err)
		return
	}
	// 死信写入带限时：它在**失败收尾路径**上，若 Redis 是黑洞
	// （网络分区而非拒连），无界的写会把 worker goroutine 挂住 ⇒
	// worker 池被拖垮、优雅停机卡住。超窗后按 best-effort 语义放弃。
	bestEffortRedis("dead_letter", func(ctx context.Context) error {
		key := fmt.Sprintf(deadLetterKey, b.buzType)
		pipe := b.client.Pipeline()
		pipe.ZAdd(ctx, key, goredis.Z{
			Score:  float64(time.Now().Unix()),
			Member: string(data),
		})
		// 按失败时间只保留最近 deadLetterMaxLen 条
		pipe.ZRemRangeByRank(ctx, key, 0, -deadLetterMaxLen-1)
		_, err := pipe.Exec(ctx)
		return err
	})
}

// bestEffortRedis 跑一个「尽力而为」的 Redis 写，最多等 bestEffortTimeout。
//
// **为什么不能只靠 ctx deadline**：go-redis 默认**丢弃**调用方的 ctx deadline
// （Options.ContextTimeoutEnabled 默认为 false ⇒ baseClient.context 返回
// context.Background()），阻塞读只受客户端自身的 ReadTimeout 约束。调用方把
// ReadTimeout 调大（长连接场景常见）时，收尾路径上的写会把 worker 挂到
// ReadTimeout（可到分钟级）⇒ worker 池被拖垮、优雅停机卡住。
//
// 这里显式限时：超时即放弃（best-effort 语义：失败只记日志），worker 立刻
// 回到池里；被放弃的 goroutine 会在客户端自身的 ReadTimeout 到点后自行结束
// （连接随之归还连接池，不会泄漏）。
func bestEffortRedis(op string, fn func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), bestEffortTimeout)
	defer cancel()

	done := make(chan struct{})
	async.GoSafe(func() {
		defer close(done)
		if err := fn(ctx); err != nil {
			log.Error("best-effort redis write failed", "op", op, "err", err)
		}
	})

	select {
	case <-done:
	case <-ctx.Done():
		log.Warn("best-effort redis write timed out, abandoned", "op", op, "timeout", bestEffortTimeout)
	}
}

// parseTask 解析任务数据。
func (b *RedisBackend) parseTask(data string) *timertask.Task {
	var task timertask.Task
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		log.Error("unmarshal error", "err", err)
		return nil
	}
	return &task
}

// PendingCount 返回队列中待处理任务数（所有分片的 ZSet 大小之和）。
func (b *RedisBackend) PendingCount(ctx context.Context) (int64, error) {
	var total int64
	for i := 0; i < b.buckets; i++ {
		count, err := b.client.ZCard(ctx, b.shardKey(i)).Result()
		if err != nil {
			return total, fmt.Errorf("pending count shard %d: %w", i, err)
		}
		total += count
	}
	return total, nil
}

// LocalPendingCount 返回本地队列中待处理任务数。
func (b *RedisBackend) LocalPendingCount() int {
	return len(b.localQueue)
}

func (b *RedisBackend) taskKey(taskID string) string {
	shard := 0
	if len(taskID) > 0 {
		// 分片下标必须走 shardIndex（**先在 uint32 空间取模再转 int**）：
		// 直接写 `int(crc32.ChecksumIEEE(...)) % b.buckets` 在 32 位平台上
		// 会按位截断成负数 ⇒ 任务被写进 `{timertask.x}.shard.-3` 这样的 key，
		// 而消费/回扫只扫 shard.0 … shard.N-1 ⇒ 任务永远不会被执行，也不进死信。
		shard = shardIndex(crc32.ChecksumIEEE([]byte(taskID)), b.buckets)
	}
	return fmt.Sprintf(taskKeyPattern, b.buzType, shard)
}

func (b *RedisBackend) shardKey(shard int) string {
	return fmt.Sprintf(taskKeyPattern, b.buzType, shard)
}
