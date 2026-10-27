package redis

import (
	"context"
	"encoding/json"
	"fmt"
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
	deadline := now + int64(b.visibilityTimeout/time.Second)
	if b.visibilityTimeout < time.Second {
		deadline = now + 1
	}
	result, err := claimLeaseScript.Run(ctx, b.client, []string{shardKey, b.processingKey()},
		member, now, deadline).Int()
	if err != nil {
		return false, err
	}
	return result > 0, nil
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
func (b *RedisBackend) requeueLeasesOnShutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b.reapExpiredLeases(ctx)
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
