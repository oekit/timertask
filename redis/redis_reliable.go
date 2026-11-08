package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// processingKeyPattern 是 at-least-once 模式下的租约集（全局按 buzType）。
// score = 租约到期时间（Unix 秒）。
const processingKeyPattern = "{timertask.%s}.processing"

// 租约时长的默认值由父模块 timertask.WithAtLeastOnce / WithVisibilityTimeout
// 统一提供（经 timertask.ResolveOptions 下发），本模块不重复定义。

// claimLeaseScript 原子 claim：score 到期则从分片 ZSet 移入 processing 租约集。
//
// KEYS[1]=shard zset, KEYS[2]=processing
// ARGV[1]=member, ARGV[2]=now, ARGV[3]=lease deadline (unix sec)
var claimLeaseScript = goredis.NewScript(`
	local score = redis.call('zscore', KEYS[1], ARGV[1])
	if score and tonumber(score) <= tonumber(ARGV[2]) then
		redis.call('zrem', KEYS[1], ARGV[1])
		redis.call('zadd', KEYS[2], tonumber(ARGV[3]), ARGV[1])
		return 1
	else
		return 0
	end
`)

func (b *RedisBackend) processingKey() string {
	return fmt.Sprintf(processingKeyPattern, b.buzType)
}

// ackTask 在 handler 成功（或终态死信）后删除租约记录。
func (b *RedisBackend) ackTask(ctx context.Context, member string) {
	if err := b.client.ZRem(ctx, b.processingKey(), member).Err(); err != nil {
		log.Error("ack ZRem failed", "member", member, "err", err)
	}
}

// claimTaskWithLease 将到期任务原子移入 processing 租约集。
// 返回 true 表示本实例 claim 成功。
func (b *RedisBackend) claimTaskWithLease(ctx context.Context, shardKey, member string, now int64) (bool, error) {
	result, err := claimLeaseScript.Run(ctx, b.client, []string{shardKey, b.processingKey()},
		member, now, b.leaseDeadline(now)).Int()
	if err != nil {
		return false, err
	}
	return result > 0, nil
}

// leaseDeadline 计算租约到期时间（Unix 秒）：now + visibilityTimeout
// （秒级整除丢弃亚秒部分——租约集 score 是秒精度）。
//
// 短租补偿：配置不足 2s 时按 2s 计。score 的比较（回扫 ZRangeByScore
// Max=now）也是整秒粒度——若租约长度只领先 now 整 1 秒，续租间隔 < 1s 时
// 「上一次续租 → 本次回扫」之间跨过一个秒边界就会让 score == now，
// 活跃租约被误判过期重投。长度 ≥ 2s 保证跨边界后仍严格大于 now。
func (b *RedisBackend) leaseDeadline(now int64) int64 {
	secs := int64(b.visibilityTimeout / time.Second)
	if secs < 2 {
		return now + 2
	}
	return now + secs
}

// renewLoop 在 handler 执行期间周期续租（#93/#95）：把 processing 集中
// 本 member 的租约 deadline 推到 now+visibilityTimeout。
//
// 此前 claim 后租约永不续期：跑得比 visibilityTimeout 慢但最终成功的
// handler 到点必被 reapExpiredLeases 重投（RetryCount++），另一实例立刻
// 再 claim——**必现**重复投递，且首次成功后的 Ack 拦不住已重投的副本。
// 续租后「租约到期 = 持有者已死」的推断才真正成立（活着的持有者会心跳）。
//
// ZAddArgs 用 XX（仅更新已存在的 member）：租约已被 Ack（ZRem）或被回扫
// 摘除后，迟到的续租不会把 member 复活成孤儿租约。
//
// 续租调用限时（#122）：本循环位于 worker 的**关键退出路径**上——processTask
// 收尾要 join 本循环，裸 ZAddArgs 阻塞在黑洞（网络分区而非拒连）里时 join
// 只能等客户端 ReadTimeout 解救：持续黑洞下每个任务收尾都被拖一截
// ReadTimeout（吞吐塌方），优雅停机时所有 worker 的退出 defer 同时被卡。
// 与 Ack/死信同一纪律：收尾路径上的 Redis 调用必须限时。限时取
// min(2s, interval)——正常续租是单命令毫秒级返回；超窗放弃不破坏语义：
// 租约交给回扫兜底（at-least-once 退化为旧行为，不会丢）。join 因此有界：
// 至多等一个限时窗口。
func (b *RedisBackend) renewLoop(ctx context.Context, member string, stop <-chan struct{}) {
	// 续租周期取租约的 1/3（与回扫周期同比例）：任一跳的抖动都不会让
	// deadline 落后于 now。
	interval := b.visibilityTimeout / 3
	if interval < 20*time.Millisecond {
		interval = 20 * time.Millisecond
	}
	renewTimeout := interval
	if renewTimeout > 2*time.Second {
		renewTimeout = 2 * time.Second
	}
	// 单飞闸（#122）：限时是弃子实现——被放弃的调用还会卡在客户端里直到
	// 客户端自身的 ReadTimeout。interval 远小于 ReadTimeout 的配置下，若不
	// 设闸，每个 tick 都会再弃一个卡死的调用，堆积弃子 goroutine 与连接。
	// 闸保证同一时刻至多一个在途续租调用：在途未归（卡死中）时后续 tick
	// 直接跳过。闸的释放在**真实调用**返回时（含被弃子后迟到的返回），
	// 而不是弃子判定时。
	var inFlight atomic.Bool
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !inFlight.CompareAndSwap(false, true) {
				continue
			}
			if err := bestEffortRedisTimeout("lease_renew", renewTimeout, func(callCtx context.Context) error {
				defer inFlight.Store(false)
				return b.client.ZAddArgs(callCtx, b.processingKey(), goredis.ZAddArgs{
					XX: true,
					Members: []goredis.Z{{
						Score:  float64(b.leaseDeadline(time.Now().Unix())),
						Member: member,
					}},
				}).Err()
			}); err != nil {
				// 续租失败只记日志：下个周期重试；连续失败到租约真正过期时，
				// 由回扫兜底重投（at-least-once 语义退化为旧行为，不会丢）。
				log.Error("lease renew failed", "member", member, "err", err)
			}
		}
	}
}

// reapExpiredLeases 把租约到期的任务按 RetryCount 重投回分片 ZSet。
// 崩溃前未 Ack 的任务由本回扫捡回，实现 at-least-once（可容忍重复，调用方幂等）。
func (b *RedisBackend) reapExpiredLeases(ctx context.Context) {
	now := time.Now().Unix()
	pkey := b.processingKey()
	members, err := b.client.ZRangeByScore(ctx, pkey, &goredis.ZRangeBy{
		Min:    "0",
		Max:    fmt.Sprintf("%d", now),
		Offset: 0,
		Count:  defaultBatch,
	}).Result()
	if err != nil {
		if err != goredis.Nil {
			log.Error("reap ZRangeByScore error", "err", err)
		}
		return
	}
	for _, member := range members {
		task := b.parseTask(member)
		if task == nil {
			// 脏数据：直接摘掉租约，避免反复回扫
			b.ackTask(ctx, member)
			continue
		}
		// 负数 MaxRetry 表示「不重试」，与 worker 侧 processTask 的归一口径
		// 一致（否则同一份任务在两条路径上的重试语义会分叉）。
		normalizeRetry(task)
		task.RetryCount++
		// 判定「重试次数耗尽」**不能**再加 `task.MaxRetry > 0` 的守卫：
		// MaxRetry<=0 时该守卫会让本分支永远不可达 ⇒ 租约到期后任务被无限
		// 重投（既不进死信、也不停止）——而 worker 侧对同一份任务早就判过
		// 「执行一次、不重试」了。
		if task.RetryCount > task.MaxRetry {
			// 租约重投次数耗尽 → 死信 + Ack
			log.Error("lease requeue exhausted", "taskID", task.ID, "retryCount", task.RetryCount)
			b.sendToDeadLetter(task)
			b.ackTask(ctx, member)
			if b.onFailure != nil {
				b.onFailure(task, fmt.Errorf("lease expired after %d attempts", task.RetryCount))
			}
			continue
		}
		data, err := json.Marshal(task)
		if err != nil {
			log.Error("reap marshal failed", "taskID", task.ID, "err", err)
			continue
		}
		key := b.taskKey(task.ID)
		// 先摘租约再回投，失败则任务丢失——用 pipeline 降低窗口；
		// 回投失败会记日志，仍优于静默。
		pipe := b.client.Pipeline()
		pipe.ZRem(ctx, pkey, member)
		pipe.ZAdd(ctx, key, goredis.Z{Score: float64(task.EndTime), Member: string(data)})
		if _, err := pipe.Exec(ctx); err != nil {
			log.Error("reap requeue failed", "taskID", task.ID, "err", err)
			continue
		}
		log.Warn("lease expired, task requeued", "taskID", task.ID, "retryCount", task.RetryCount)
	}
}

// requeueLeasesOnShutdown 停机时把本地 processing 中属于本进程可能未完成的
// 租约任务提前摘掉并回投（best-effort）。任一实例的回扫也能捡到，这里是加速。
//
// 整轮回扫限时 requeueShutdownWindow（#121）：reapExpiredLeases 内部是
// ZRangeByScore + 逐 member 串行 pipeline，黑洞（网络分区）下 N 个租约原本
// 会串行阻塞到 N×ReadTimeout，把 Start 退出路径挂死。限时是真实墙钟约束
// （go-redis 默认丢弃调用方 ctx deadline，裸 WithTimeout 是虚构的），套
// bestEffortRedisTimeout（select + 弃子 goroutine）实现。超窗放弃不破坏
// 语义：租约仍在 processing 集里，由任意实例的 reapLoop 按到期时间兜底捡回。
func (b *RedisBackend) requeueLeasesOnShutdown() {
	_ = bestEffortRedisTimeout("requeue_leases_shutdown", requeueShutdownWindow, func(ctx context.Context) error {
		b.reapExpiredLeases(ctx)
		return nil
	})
}

// reapLoop 周期回扫过期租约。
func (b *RedisBackend) reapLoop(ctx context.Context) {
	// 回扫周期取租约的 1/3，避免过期任务长时间滞留
	interval := b.visibilityTimeout / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			b.requeueLeasesOnShutdown()
			return
		case <-b.stopChan:
			b.requeueLeasesOnShutdown()
			return
		case <-ticker.C:
			b.reapExpiredLeases(ctx)
		}
	}
}

// mustJSON 序列化 task（测试辅助）。
func mustJSON(t *timertask.Task) string {
	data, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return string(data)
}
