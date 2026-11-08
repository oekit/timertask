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

// requeueShutdownWindow 是三条停机回灌路径（requeuePendingOnShutdown /
// requeueClaimOnShutdown / requeueLeasesOnShutdown）共享的回灌窗口。
//
// 超窗后按 doc 声明的 at-most-once 语义放弃：未回灌的任务/租约交给 Redis
// 侧的回扫（reapExpiredLeases / 下个周期）兜底。
//
// ⚠️ 窗口必须是**真实墙钟约束**（#121）：go-redis 默认丢弃调用方的 ctx
// deadline（Options.ContextTimeoutEnabled 默认 false），裸
// context.WithTimeout 对它是虚构的——所以三条路径的回灌调用都套
// bestEffortRedisTimeout（select + 弃子 goroutine）实现真实限时。
//
// 用 var 而非 const：测试需要把它调小以在毫秒级内复现「黑洞」场景。
var requeueShutdownWindow = 5 * time.Second

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
	// started/stopped 是 Start 的状态机（#124），语义与 LocalBackend 一致，
	// 由 mu 保护：stopChan 关闭后不重建，不做状态机的话二次 Start 会拉起一组
	// 立刻哑火的消费者（本实例消费吞吐静默归零），Start 却阻塞在新 ctx 上
	// 看起来健康。状态机让「第二次 Start」要么报错、要么（首次）真的开始消费。
	mu      sync.Mutex
	started bool
	stopped bool
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
//
// ZSet member 是任务的**规范化 JSON**：CreateTime 钉为 EndTime（#75）——此前
// member 是 task 的原样序列化，而 Send 缺省给 CreateTime 填 now，同一任务同
// fire_ts 拍两次 Send ⇒ CreateTime 不同 ⇒ member 不同 ⇒ ZSet 两条 ⇒ **双跑**。
// 规范化后「同拍重发 = 同 member」（ZAdd 同 member 更新 score 而非新增条目），
// 去重从调用方纪律（下游被迫钉死 CreateTime=EndTime 换字节一致）下沉为库内
// 不变量。语义副作用：消费侧拿到的 task.CreateTime == task.EndTime——「创建
// 时刻」在 ZSet member 语义里不再存在，需要真创建时刻的调用方自存（业务字段）。
//
// #97：不改写调用方的 Task——CreateTime/MaxRetry 的缺省补齐与 member 规范化
// 都落在内部副本上（Task 全为值字段，浅拷贝即全量拷贝），调用方提交后继续
// 读/复用自己的 Task 结构不会看到字段被引擎改写。
//
// 后端停机后（Start 已返回）返回 timertask.ErrBackendStopped（#124）：实例
// 的消费循环已经退出，继续投递只会制造「写入成功、本实例零消费」的错觉；
// 要继续投递请重建实例或换用存活实例。
func (b *RedisBackend) Send(ctx context.Context, task *timertask.Task) error {
	if task.ID == "" {
		return timertask.ErrEmptyTaskID
	}
	b.mu.Lock()
	stopped := b.stopped
	b.mu.Unlock()
	if stopped {
		return timertask.ErrBackendStopped
	}
	// 规范化 member：拷贝后补 MaxRetry 缺省、钉 CreateTime=EndTime
	// （CreateTime 的补 now 随 #75 的钉死早已到不了 member，随 #97 一并移除）；
	// 其余字段与调用方输入逐字节一致——两次 Send 同参 ⇒ 同 member。
	c := *task
	if c.MaxRetry == 0 {
		c.MaxRetry = b.retryCount
	}
	c.CreateTime = c.EndTime
	data, err := json.Marshal(&c)
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

// Remove 撤销一个已提交、**尚未触发**的延迟任务（#75）。
// 返回实际撤销的条数（0 = 无匹配的待触发条目，幂等可重入）。
//
// 实现按 (taskID → 分片 key，fireTS → score 精确窗口) 扫描：member 是任务全量
// JSON（含业务字段），调用方只有 (ID, fireTS) 无法逐字节重建 member，故用
// 「score 精确窗口（ZRangeByScore min=max）+ 解析比对 task_id」定位后 ZRem——
// 窗口内条目数 = 该分片同一秒到期的任务数，量级可忽略。
//
// 语义边界：
//   - 只覆盖「尚未被消费扫描 claim」的条目；已 claim / 本地队列 / 重试中 / 已进
//     死信的任务不受影响（撤销要赶在 fireTS 之前——窗口期语义即本 API 的存在理由）；
//   - 与消费扫描竞态（fireTS ± 扫描周期）：先到者得——Remove 赢 = 任务不触发；
//     claim 赢 = 正常触发（返回 0）；
//   - 历史格式 member（旧版全量 JSON、CreateTime≠EndTime）解析不受影响，
//     一样按 ID 匹配可撤销。
func (b *RedisBackend) Remove(ctx context.Context, taskID string, endTS int64) (int64, error) {
	if taskID == "" {
		return 0, timertask.ErrEmptyTaskID
	}
	key := b.taskKey(taskID)
	members, err := b.client.ZRangeByScore(ctx, key, &goredis.ZRangeBy{
		Min: strconv.FormatInt(endTS, 10),
		Max: strconv.FormatInt(endTS, 10),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("timertask: ZRANGEBYSCORE %s: %w", key, err)
	}
	var removed int64
	for _, m := range members {
		t := b.parseTask(m)
		if t == nil || t.ID != taskID {
			continue
		}
		n, err := b.client.ZRem(ctx, key, m).Result()
		if err != nil {
			return removed, fmt.Errorf("timertask: ZREM %s: %w", key, err)
		}
		removed += n
	}
	if removed > 0 {
		log.Info("task removed", "taskID", taskID, "endTime", endTS, "removed", removed)
	}
	return removed, nil
}

// Start 启动任务消费（阻塞）。
// ctx 取消时所有 goroutine 退出。
//
// 实例一次性（#124）：Start 返回后实例进入停机态——再次调用返回
// timertask.ErrBackendStopped（首个 Start 尚未返回时的并发重复调用返回
// already started 错误），不会像旧实现那样拉起一组因 stopChan 已关闭而
// 立刻退出的哑火 goroutine。需要重启任务消费请重建实例。
func (b *RedisBackend) Start(ctx context.Context) error {
	// #124 状态机：入口先做状态检查并登记 started。检查必须在拉起 goroutine
	// 之前，否则二次调用同样会拉起一组必然哑火的消费者。
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return timertask.ErrBackendStopped
	}
	if b.started {
		b.mu.Unlock()
		// 与根包 errBackendAlreadyStarted 同串（独立 module 无法访问未导出符号）。
		return errors.New("timertask: backend already started")
	}
	b.started = true
	b.mu.Unlock()

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
	// #124：停机收尾完成后登记 stopped——此后的二次 Start / Send 都显式报错
	// （timertask.ErrBackendStopped）。
	b.mu.Lock()
	b.stopped = true
	b.mu.Unlock()
	return ctx.Err()
}

// requeuePendingOnShutdown 将本地队列中尚未处理的任务回灌 Redis（ZADD 回原分片，
// 保留原 EndTime），重启后由任意实例继续消费。仅覆盖优雅停机路径；
// 进程崩溃时队列内任务仍会丢失（见包文档的 at-most-once 语义）。
//
// 回灌窗口限时 requeueShutdownWindow：Redis 黑洞（网络分区而非拒连）时无界
// 的 ZAdd 会挂到 OS 级 TCP 超时（分钟级），worker 无法退出、Start 调用方
// 卡死。窗口是真实墙钟约束（#121）：每笔回灌套 bestEffortRedisTimeout
// （go-redis 默认丢弃调用方 ctx deadline，裸 WithTimeout 对黑洞客户端是
// 虚构的），预算取剩余窗口并逐次收紧——总耗时必然 ≤ 窗口。超窗后剩余任务
// 按 doc 声明的 at-most-once 语义丢弃。
func (b *RedisBackend) requeuePendingOnShutdown() {
	requeued := 0
	deadline := time.Now().Add(requeueShutdownWindow)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if requeued > 0 {
				log.Warn("requeue window expired, remaining tasks dropped (at-most-once)",
					"requeued", requeued)
			}
			return
		}
		select {
		case c := <-b.localQueue:
			if c == nil || c.task == nil {
				continue
			}
			okCh := make(chan bool, 1)
			err := bestEffortRedisTimeout("requeue_pending_shutdown", remaining, func(callCtx context.Context) error {
				okCh <- b.requeueClaim(callCtx, c)
				return nil
			})
			// err == nil ⇔ 回灌调用在窗口内真实完成（闭包恒返回 nil）；
			// 超窗弃子时闭包可能仍在途，不能读 okCh（否则与迟到的写入竞态）。
			if err == nil && <-okCh {
				requeued++
			}
		default:
			if requeued > 0 {
				log.Info("requeued pending tasks on shutdown", "count", requeued)
			}
			return
		}
	}
}

// requeueClaim 把一个已 claim、尚未处理的任务回灌 Redis（ZADD 回原分片，
// 保留原 EndTime；at-least-once 时同时摘掉 processing 租约），返回是否成功。
// 优雅停机的两条路径复用：localQueue 回灌（requeuePendingOnShutdown）与
// claim 后未及入队的在途任务（consumeRemoteOnce 的停机窗口，#96）。
func (b *RedisBackend) requeueClaim(ctx context.Context, c *claim) bool {
	task := c.task
	data, err := json.Marshal(task)
	if err != nil {
		log.Error("requeue marshal failed", "taskID", task.ID, "err", err)
		return false
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
		return false
	}
	return true
}

// requeueClaimOnShutdown 在停机窗口内回灌单个在途 claim（限时
// requeueShutdownWindow，与 requeuePendingOnShutdown 的回灌窗口一致：Redis
// 黑洞时不把退出路径挂死，超窗按 at-most-once 语义放弃）。
//
// #121：限时是真实墙钟约束——go-redis 默认丢弃调用方 ctx deadline
// （ContextTimeoutEnabled=false），裸 WithTimeout 对黑洞客户端是虚构的
// （实测 deadline ctx 下仍阻塞整段 ReadTimeout），所以套 bestEffortRedisTimeout
// （select + 弃子 goroutine）实现。
func (b *RedisBackend) requeueClaimOnShutdown(c *claim) {
	_ = bestEffortRedisTimeout("requeue_claim_shutdown", requeueShutdownWindow, func(ctx context.Context) error {
		if b.requeueClaim(ctx, c) {
			log.Info("requeued in-flight claim on shutdown", "taskID", c.task.ID)
		}
		return nil
	})
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
					// #96：claim（deleteScript 已原子删除/移入租约集）与入队之间
					// 遇停机，直接 return 会把任务丢在「既不在 ZSet 也不在
					// localQueue」的地方——requeuePendingOnShutdown 只回灌
					// localQueue 里的任务，捡不到在途的这一份。先回灌 Redis
					// 再退出。
					b.requeueClaimOnShutdown(&claim{task: task, member: data})
					return
				case <-b.stopChan:
					b.requeueClaimOnShutdown(&claim{task: task, member: data})
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
	// at-least-once：handler 执行期间周期续租（#93/#95）。defers LIFO——
	// 本 defer 先于上面的 Ack defer 执行：续租先停干净，Ack 不会与续租竞争
	// （ZAddArgs XX 也兜住了跨进程的迟到续租不复活已 Ack 的租约）。
	// join 有界（#122）：renewLoop 内的续租调用自带弃子限时
	// （min(2s, interval)，见 renewLoop），收尾 join 至多等一个限时窗口，
	// 不会被黑洞里的续租写卡到客户端 ReadTimeout。
	if b.delivery == timertask.DeliveryAtLeastOnce {
		stopRenew := make(chan struct{})
		renewExited := make(chan struct{})
		async.GoSafe(func() {
			defer close(renewExited)
			b.renewLoop(ctx, c.member, stopRenew)
		})
		defer func() {
			close(stopRenew)
			<-renewExited
		}()
	}
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
func bestEffortRedis(op string, fn func(ctx context.Context) error) {
	_ = bestEffortRedisTimeout(op, bestEffortTimeout, fn)
}

// bestEffortRedisTimeout 跑一个「尽力而为」的 Redis 调用，最多等 timeout
// （#121 起收尾路径统一走这里：三条停机回灌路径与续租调用都需要自定义窗口，
// 不再只有全局 bestEffortTimeout 一档）。
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
//
// 返回 fn 的错误；超时放弃时返回 context.DeadlineExceeded。放弃后调用方
// 不得再读取 fn 闭包捕获的结果（fn 可能仍在途、与迟到的写入竞态）。
func bestEffortRedisTimeout(op string, timeout time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})
	var fnErr error
	async.GoSafe(func() {
		defer close(done)
		fnErr = fn(ctx)
		if fnErr != nil {
			log.Error("best-effort redis write failed", "op", op, "err", fnErr)
		}
	})

	select {
	case <-done:
		return fnErr
	case <-ctx.Done():
		log.Warn("best-effort redis write timed out, abandoned", "op", op, "timeout", timeout)
		return ctx.Err()
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
