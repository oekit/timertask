package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oekit/logx"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// processingKeyPattern 是 at-least-once 模式下的租约集（全局按 buzType）。
// score = 租约到期时间（Unix 秒）。
const processingKeyPattern = "{timertask.%s}.processing"

// retryCountKeyPattern 是租约回投重试计数的 hash（#188）：field =
// "<task_id>:<end_time>"，value = 该任务累计经历的租约到期回投次数。
//
// retry_count 必须移出 ZSet member：member 的去重键是整个字节串，
// 回投路径把 retry_count+1 写回 member 会与 Send 产出的 member 差一个
// 字段 ⇒ 同一任务同 fire_ts 两条条目 ⇒ 业务 handler 双跑（#76 的
// 「同拍重发 = 同 member」不变量在回投路径失效）。member 归零携带
// retry_count（canonicalMember），计数单源在本 hash。
const retryCountKeyPattern = "{timertask.%s}.retry_count"

// 租约时长的默认值由父模块 timertask.WithAtLeastOnce / WithVisibilityTimeout
// 统一提供（经 timertask.ResolveOptions 下发），本模块不重复定义。

// retryCountKey 返回 retry-count hash 的 key。
func (b *RedisBackend) retryCountKey() string {
	return fmt.Sprintf(retryCountKeyPattern, b.buzType)
}

// retryCountField 返回任务在 retry-count hash 里的 field。带 end_time 维度：
// 同一 task_id 换 fire_ts 重投是「新的一轮预算」，字段天然隔离；终态
// （成功 / 死信）由 ackTask 统一 HDel 清理，不留孤儿字段。
func (b *RedisBackend) retryCountField(task *timertask.Task) string {
	return task.ID + ":" + strconv.FormatInt(task.EndTime, 10)
}

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

// requeueLeaseScript 原子回投：先 ZAdd 回分片 ZSet、再 ZRem 摘 processing 租约
// （#157，reapExpiredLeases 与 requeueClaim 两条回投路径共用同一原子面）。
//
// 「先回投成功再摘租约」不能靠同一条 pipeline 的命令顺序：go-redis
// Pipeline() 非 MULTI/EXEC、逐条独立执行，回投的 ZAdd 命令级失败（OOM /
// maxmemory 逐出 / WRONGTYPE）而摘租约的 ZRem 照常执行时，任务从分片 ZSet
// 与 processing 两边都被摘掉、不进死信——永久丢失，与 at-least-once
// 「可容忍重复、不容忍丢失」的承诺相悖（#150 只反转了顺序，堵不住这条腿）。
// 脚本内 redis.call 出错即中止（后续命令不执行、已执行命令不回滚）：
//   - ZAdd 失败 ⇒ ZRem 不执行，租约保留、下轮回扫再投（不丢）；
//   - ZAdd 成功后 ZRem 失败 ⇒ 任务暂留分片与 processing 两处，下轮回扫
//     重复回投（at-least-once 允许重复）。
//
// 两个半失败腿都不丢任务。
//
// KEYS[1]=shard zset, KEYS[2]=processing
// ARGV[1]=score (EndTime), ARGV[2]=回投 member（canonicalMember 规范化产出，
// 不携带真实 retry_count——计数在 retry-count hash，#188）,
// ARGV[3]=processing member（原租约 member）
var requeueLeaseScript = goredis.NewScript(`
	redis.call('zadd', KEYS[1], tonumber(ARGV[1]), ARGV[2])
	redis.call('zrem', KEYS[2], ARGV[3])
	return 1
`)

func (b *RedisBackend) processingKey() string {
	return fmt.Sprintf(processingKeyPattern, b.buzType)
}

// ackTask 在 handler 成功（或终态死信）后删除租约记录，并顺手清掉
// retry-count hash 字段（#188）：两条终态路径（worker 侧成功/死信、回扫侧
// 死信）共用本收口，避免孤儿计数残留。毒丸 member 解析不出任务时跳过
// HDel——无 ID 可清，hash 字段本来也没建过。
func (b *RedisBackend) ackTask(ctx context.Context, member string) {
	if err := b.client.ZRem(ctx, b.processingKey(), member).Err(); err != nil {
		log.Error("ack ZRem failed", "member", member, logx.ErrKey, err)
	}
	var task timertask.Task
	if err := json.Unmarshal([]byte(member), &task); err == nil && task.ID != "" {
		if err := b.client.HDel(ctx, b.retryCountKey(), b.retryCountField(&task)).Err(); err != nil {
			log.Error("ack HDel retry count failed", "taskID", task.ID, logx.ErrKey, err)
		}
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
	return now + max(secs, 2)
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
	interval := max(b.visibilityTimeout/3, 20*time.Millisecond)
	renewTimeout := min(interval, 2*time.Second)
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
						Score:  float64(b.leaseDeadline(b.now().Unix())), // #152
						Member: member,
					}},
				}).Err()
			}); err != nil {
				// 续租失败只记日志：下个周期重试；连续失败到租约真正过期时，
				// 由回扫兜底重投（at-least-once 语义退化为旧行为，不会丢）。
				log.Error("lease renew failed", "member", member, logx.ErrKey, err)
			}
		}
	}
}

// reapExpiredLeases 把租约到期的任务按 RetryCount 重投回分片 ZSet。
// 崩溃前未 Ack 的任务由本回扫捡回，实现 at-least-once（可容忍重复，调用方幂等）。
func (b *RedisBackend) reapExpiredLeases(ctx context.Context) {
	now := b.now().Unix() // #152：过期判定走注入时钟
	pkey := b.processingKey()
	members, err := b.client.ZRangeByScore(ctx, pkey, &goredis.ZRangeBy{
		Min:    "0",
		Max:    fmt.Sprintf("%d", now),
		Offset: 0,
		Count:  defaultBatch,
	}).Result()
	if err != nil {
		if err != goredis.Nil {
			log.Error("reap ZRangeByScore error", logx.ErrKey, err)
		}
		return
	}
	for _, member := range members {
		task := b.parseTask(member)
		if task == nil {
			// #175：毒丸租约不能静默丢弃——此前直接摘租约，绕过专为止损
			// 取证设计的死信 ZSet。原样进死信（score=当前时刻），再摘租约
			// 止损避免反复回扫。顺序先死信后摘租约：半失败时最坏是死信
			// 重复条目，不会丢取证材料（与 requeueLeaseScript 的
			// 「先回投再摘」同一取舍）。
			b.sendToDeadLetterRaw(member)
			b.ackTask(ctx, member)
			continue
		}
		// 负数 MaxRetry 表示「不重试」，与 worker 侧 processTask 的归一口径
		// 一致（否则同一份任务在两条路径上的重试语义会分叉）。归一单源
		// （#169）：根包导出的 timertask.NormalizeRetry。
		timertask.NormalizeRetry(task)
		// 重试计数走 retry-count hash（#188）：member 不再携带真实
		// retry_count（字节稳定，「回投 member = Send member」），计数单源
		// 在 hash 里推进。HIncrBy 失败时租约保留、下轮回扫再投（at-least-once，
		// 计数未推进只是少记一轮，不丢任务）。
		count, err := b.client.HIncrBy(ctx, b.retryCountKey(), b.retryCountField(task), 1).Result()
		if err != nil {
			log.Error("reap incr retry count failed", "taskID", task.ID, logx.ErrKey, err)
			continue
		}
		task.RetryCount = int(count)
		// 判定「重试次数耗尽」**不能**再加 `task.MaxRetry > 0` 的守卫：
		// MaxRetry<=0 时该守卫会让本分支永远不可达 ⇒ 租约到期后任务被无限
		// 重投（既不进死信、也不停止）——而 worker 侧对同一份任务早就判过
		// 「执行一次、不重试」了。
		if task.RetryCount > task.MaxRetry {
			// 租约重投次数耗尽 → 死信 + Ack（ackTask 顺手清 hash 字段）
			log.Error("lease requeue exhausted", "taskID", task.ID, "retryCount", task.RetryCount)
			b.sendToDeadLetter(task)
			b.ackTask(ctx, member)
			if b.onFailure != nil {
				b.onFailure(task, fmt.Errorf("lease expired after %d attempts", task.RetryCount))
			}
			continue
		}
		// 回投 member 单源 canonicalMember（#188）：与 Send 产出同一字节串，
		// 生产者同拍重发命中 ZAdd 的 score 更新而非新增条目。
		data, err := b.canonicalMember(task)
		if err != nil {
			log.Error("reap marshal failed", "taskID", task.ID, logx.ErrKey, err)
			continue
		}
		key := b.taskKey(task.ID)
		// 回投 + 摘租约走原子脚本（#157）：「先回投成功再摘租约」由服务端保证
		// （细节见 requeueLeaseScript 注释）——回投写失败时租约保留、下轮回扫
		// 再投；不可用同一条 pipeline 的命令顺序冒充原子面：非 MULTI/EXEC
		// 逐条独立执行，「ZAdd 失败 + ZRem 成功」⇒ 两处皆空、永久丢失。
		if err := requeueLeaseScript.Run(ctx, b.client, []string{key, pkey},
			task.EndTime, data, member).Err(); err != nil {
			log.Error("reap requeue failed", "taskID", task.ID, logx.ErrKey, err)
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
	interval := max(b.visibilityTimeout/3, time.Second)
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
