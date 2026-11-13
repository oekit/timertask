package redis

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// newPoisonTestBackend 构造可指定投递语义的测试后端，并把分片分配钉在
// shard 0（消费路径确定性：不经心跳/分配循环）。返回后端与 miniredis 实例
// （种毒丸用）及 handler 调用计数。
func newPoisonTestBackend(t *testing.T, delivery timertask.DeliveryMode) (*RedisBackend, func() int32) {
	t.Helper()
	b, _, calls := newPoisonTestBackendMR(t, delivery)
	return b, calls
}

// newPoisonTestBackendMR 同 newPoisonTestBackend 但返回 miniredis 实例
// （#197 需要对死信 key 注入写失败）。
func newPoisonTestBackendMR(t *testing.T, delivery timertask.DeliveryMode) (*RedisBackend, *miniredis.Miniredis, func() int32) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	var calls atomic.Int32
	cfg := timertask.ResolveOptions([]timertask.TimerTaskOption{
		timertask.WithBuckets(4),
		timertask.WithWorkers(1),
		timertask.WithRetry(0, 10*time.Millisecond),
		timertask.WithDeliveryMode(delivery),
	}...)
	b := &RedisBackend{
		client:            c,
		buzType:           "test",
		handler:           func(context.Context, *timertask.Task) error { calls.Add(1); return nil },
		buckets:           cfg.Buckets,
		workers:           cfg.Workers,
		retryCount:        cfg.RetryCount,
		retryDelay:        cfg.RetryDelay,
		delivery:          cfg.Delivery,
		visibilityTimeout: cfg.VisibilityTimeout,
		localQueue:        make(chan *claim, 1000),
		stopChan:          make(chan struct{}),
		shardManager:      NewShardManager("test", c, cfg.Buckets),
	}
	// 钉死分片：只扫 shard 0（毒丸也种在 shard 0）
	b.shardManager.mu.Lock()
	b.shardManager.myShards = []int{0}
	b.shardManager.mu.Unlock()
	return b, mr, calls.Load
}

// TestRedisBackend_PoisonMember_DeadLetter_AtMostOnce 钉住 #175（claim 路径，
// at-most-once）：deleteScript 原子删除后解析失败的毒丸 member 必须原样进
// 死信 ZSet——此前静默丢弃，任务三处皆无、无取证痕迹，绕过专为止损取证
// 设计的死信 ZSet。
func TestRedisBackend_PoisonMember_DeadLetter_AtMostOnce(t *testing.T) {
	b, handlerCalls := newPoisonTestBackend(t, timertask.DeliveryAtMostOnce)
	ctx := context.Background()

	poison := `{"task_id":"poison-1","broken`
	shardKey := "{timertask.test}.shard.0"
	deadKey := "{timertask.test}.dead"
	if err := b.client.ZAdd(ctx, shardKey, goredis.Z{Score: float64(time.Now().Unix() - 10), Member: poison}).Err(); err != nil {
		t.Fatal(err)
	}

	b.consumeRemoteOnce(ctx)

	// 毒丸原样进死信（不解析、不二次包装——取证要原始字节）
	members, err := b.client.ZRange(ctx, deadKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != poison {
		t.Fatalf("死信 ZSet = %v, want [%q]（#175 毒丸应原样取证）", members, poison)
	}
	// 分片 ZSet 已被 claim 清空
	if n, _ := b.client.ZCard(ctx, shardKey).Result(); n != 0 {
		t.Fatalf("分片 ZSet 残留 %d 条, want 0", n)
	}
	// 毒丸不会被执行
	if n := handlerCalls(); n != 0 {
		t.Fatalf("handler 被调用 %d 次, want 0", n)
	}
}

// TestRedisBackend_PoisonMember_DeadLetter_AtLeastOnce 钉住 #175（claim 路径，
// at-least-once）：毒丸经 claimTaskWithLease 移入租约集后解析失败——原样进
// 死信之外还必须摘租约止损（不摘会被回扫再次判脏，死信出现重复条目）。
func TestRedisBackend_PoisonMember_DeadLetter_AtLeastOnce(t *testing.T) {
	b, handlerCalls := newPoisonTestBackend(t, timertask.DeliveryAtLeastOnce)
	ctx := context.Background()

	poison := `not-json-at-all`
	shardKey := "{timertask.test}.shard.0"
	deadKey := "{timertask.test}.dead"
	if err := b.client.ZAdd(ctx, shardKey, goredis.Z{Score: float64(time.Now().Unix() - 10), Member: poison}).Err(); err != nil {
		t.Fatal(err)
	}

	b.consumeRemoteOnce(ctx)

	members, err := b.client.ZRange(ctx, deadKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != poison {
		t.Fatalf("死信 ZSet = %v, want [%q]（#175）", members, poison)
	}
	// 租约已摘（止损，不留给回扫重复判脏）
	if n, _ := b.client.ZCard(ctx, "{timertask.test}.processing").Result(); n != 0 {
		t.Fatalf("processing 租约残留 %d 条, want 0", n)
	}
	if n, _ := b.client.ZCard(ctx, shardKey).Result(); n != 0 {
		t.Fatalf("分片 ZSet 残留 %d 条, want 0", n)
	}
	if n := handlerCalls(); n != 0 {
		t.Fatalf("handler 被调用 %d 次, want 0", n)
	}
}

// TestRedisBackend_PoisonLease_ReapedToDeadLetter 钉住 #175（回扫路径）：
// 租约到期的毒丸 member 此前被静默摘租约丢弃——必须先原样进死信取证，
// 再摘租约止损。
func TestRedisBackend_PoisonLease_ReapedToDeadLetter(t *testing.T) {
	b, _ := newPoisonTestBackend(t, timertask.DeliveryAtLeastOnce)
	ctx := context.Background()

	poison := `{"task_id":"poison-lease","truncated`
	deadKey := "{timertask.test}.dead"
	// 直接种一条租约已过期的毒丸
	if err := b.client.ZAdd(ctx, "{timertask.test}.processing", goredis.Z{Score: float64(time.Now().Unix() - 10), Member: poison}).Err(); err != nil {
		t.Fatal(err)
	}

	b.reapExpiredLeases(ctx)

	members, err := b.client.ZRange(ctx, deadKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != poison {
		t.Fatalf("死信 ZSet = %v, want [%q]（#175 毒丸租约应取证后再止损）", members, poison)
	}
	if n, _ := b.client.ZCard(ctx, "{timertask.test}.processing").Result(); n != 0 {
		t.Fatalf("processing 租约残留 %d 条, want 0（止损）", n)
	}
}

// —— #197：死信写失败必须保留租约，恢复后重写成功 ——

// TestRedisBackend_DeadLetterAckFailureKeepsLease 钉住 #197 主诉：四条
// 「死信 + Ack」此前是两条独立往返——死信写被 bestEffortRedis 吞错后 Ack
// 照常执行，毒丸从 processing 摘除后三处皆无（不在分片、不在 processing、
// 不在死信），at-least-once「不容忍丢失」与「重试耗尽进持久死信」双双击穿。
// 现走同一 Lua 原子面：写失败 ⇒ 租约保留，下轮回扫重试；恢复后收敛。
func TestRedisBackend_DeadLetterAckFailureKeepsLease(t *testing.T) {
	b, mr, _ := newPoisonTestBackendMR(t, timertask.DeliveryAtLeastOnce)
	ctx := context.Background()

	poison := `{"task_id":"poison-197","broken`
	deadKey := "{timertask.test}.dead"

	// 种过期毒丸租约（score=0 必然被回扫选中）。
	if err := b.client.ZAdd(ctx, b.processingKey(), goredis.Z{
		Score:  0,
		Member: poison,
	}).Err(); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	// 注入写失败（miniredis 全局错误窗，模拟 maxmemory 逐出 / WRONGTYPE /
	// 黑洞窗口）；错误窗内客户端命令全挂，reap 的死信脚本失败即止。
	mr.SetError("ERR simulated dead letter write failure")
	b.reapExpiredLeases(ctx)
	mr.SetError("") // 关窗恢复，随后断言持久状态

	// 断言①：租约保留（修复前：Ack 照常执行，member 三处皆无）。
	// ZScore err=nil 即 member 存在（种入的 score 本就是 0）。
	if _, err := b.client.ZScore(ctx, b.processingKey(), poison).Result(); err != nil {
		t.Fatalf("★ #197 死信失败必须保留租约（下轮回扫重试）: %v", err)
	}
	// 断言②：死信 ZSet 不得有半写条目。
	if n, _ := b.client.ZCard(ctx, deadKey).Result(); n != 0 {
		t.Fatalf("死信写失败不应留半写条目, dead=%d", n)
	}

	// 恢复写入 → 下轮回扫收敛：进死信 + 摘租约。
	b.reapExpiredLeases(ctx)
	if n, _ := b.client.ZCard(ctx, deadKey).Result(); n != 1 {
		t.Fatalf("★ 恢复后死信应恰有 1 条, got %d", n)
	}
	if _, err := b.client.ZScore(ctx, b.processingKey(), poison).Result(); err == nil {
		t.Fatal("★ 恢复后租约应已摘除（收敛）")
	}
}
