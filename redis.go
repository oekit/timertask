package timertask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yi-nology/ekit-concurrency/async"
	"github.com/yi-nology/ekit-logx"
)

const (
	taskKeyPattern    = "{timertask.%s}.shard.%d"
	heartbeatKey      = "{timertask.%s}.heartbeat"
	deadLetterKey     = "{timertask.%s}.dead"
	defaultBuckets    = 128
	defaultBatch      = 300
	defaultWorkers    = 10
	defaultRetryCount = 3
	defaultRetryDelay = 5 * time.Second
	// deadLetterMaxLen 死信 ZSet 保留上限（按失败时间只保留最近 N 条），防止无限增长。
	deadLetterMaxLen = 10000
)

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
var deleteScript = redis.NewScript(`
	local score = redis.call('zscore', KEYS[1], ARGV[1])
	if score and tonumber(score) <= tonumber(ARGV[2]) then
		return redis.call('zrem', KEYS[1], ARGV[1])
	else
		return 0
	end
`)

var log = logx.Delegating("timertask")

// RedisBackend 是基于 Redis ZSet 的分布式延迟任务实现。
type RedisBackend struct {
	client     redis.UniversalClient
	buzType    string
	handler    Handler
	onFailure  func(*Task, error)
	buckets    int
	workers    int
	retryCount int
	retryDelay time.Duration

	// 分片管理
	shardManager *ShardManager

	// 本地队列
	localQueue chan *Task
	stopChan   chan struct{}
	stopOnce   sync.Once
}

// NewRedisBackend 创建基于 Redis 的延迟任务后端。
//
//   - buzType: 业务类型标识（区分不同业务的任务）
//   - client: Redis 客户端（支持单机和集群）
//   - handler: 任务处理函数
func NewRedisBackend(buzType string, client redis.UniversalClient, handler Handler, opts ...TimerTaskOption) *RedisBackend {
	cfg := applyOptions(opts)
	b := &RedisBackend{
		client:     client,
		buzType:    buzType,
		handler:    handler,
		onFailure:  cfg.onFailure,
		buckets:    cfg.buckets,
		workers:    cfg.workers,
		retryCount: cfg.retryCount,
		retryDelay: cfg.retryDelay,
		localQueue: make(chan *Task, 100000),
		stopChan:   make(chan struct{}),
	}

	// 创建分片管理器
	b.shardManager = NewShardManager(buzType, client, b.buckets)

	return b
}

// Send 提交一个延迟任务。
// task.EndTime（Unix 秒）决定任务何时被执行。
func (b *RedisBackend) Send(ctx context.Context, task *Task) error {
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
	_, err = b.client.ZAdd(ctx, key, redis.Z{
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
		case task := <-b.localQueue:
			if task == nil {
				continue
			}
			data, err := json.Marshal(task)
			if err != nil {
				log.Error("requeue marshal failed", "taskID", task.ID, "err", err)
				continue
			}
			key := b.taskKey(task.ID)
			if _, err := b.client.ZAdd(ctx, key, redis.Z{
				Score:  float64(task.EndTime),
				Member: string(data),
			}).Result(); err != nil {
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
		tasks, err := b.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
			Min:    "0",
			Max:    strconv.FormatInt(now, 10),
			Offset: 0,
			Count:  defaultBatch,
		}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				log.Error("ZRangeByScore error", "err", err)
			}
			continue
		}

		for _, data := range tasks {
			// 使用 Lua 脚本原子删除（score 相同时才删除）
			result, err := deleteScript.Run(ctx, b.client, []string{key}, data, now).Int()
			if err != nil {
				log.Error("delete script error", "err", err)
				continue
			}
			if result > 0 {
				task := b.parseTask(data)
				if task == nil {
					log.Error("failed to parse task from ZSet, data discarded", "key", key)
					continue
				}
				// 推入本地队列
				select {
				case b.localQueue <- task:
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
		case task := <-b.localQueue:
			if task == nil {
				continue
			}
			b.processTask(ctx, task)
		}
	}
}

// processTask 处理单个任务，支持重试。
func (b *RedisBackend) processTask(ctx context.Context, task *Task) {
	var err error
	normalizeRetry(task)
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
}

// sendToDeadLetter 将重试耗尽的任务写入持久死信 ZSet（score 为失败时间戳），
// 供人工排查或补偿任务消费。写入为 best-effort：失败仅记录日志，不阻断 onFailure。
// 失败原因已在调用方日志中携带，死信只存任务本体。
func (b *RedisBackend) sendToDeadLetter(task *Task) {
	data, err := json.Marshal(task)
	if err != nil {
		log.Error("dead letter marshal failed", "taskID", task.ID, "err", err)
		return
	}
	key := fmt.Sprintf(deadLetterKey, b.buzType)
	pipe := b.client.Pipeline()
	pipe.ZAdd(context.Background(), key, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: string(data),
	})
	// 按失败时间只保留最近 deadLetterMaxLen 条
	pipe.ZRemRangeByRank(context.Background(), key, 0, -deadLetterMaxLen-1)
	if _, err := pipe.Exec(context.Background()); err != nil {
		log.Error("dead letter write failed", "taskID", task.ID, "err", err)
	}
}

// parseTask 解析任务数据。
func (b *RedisBackend) parseTask(data string) *Task {
	var task Task
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
		shard = int(crc32.ChecksumIEEE([]byte(taskID))) % b.buckets
	}
	return fmt.Sprintf(taskKeyPattern, b.buzType, shard)
}

func (b *RedisBackend) shardKey(shard int) string {
	return fmt.Sprintf(taskKeyPattern, b.buzType, shard)
}
