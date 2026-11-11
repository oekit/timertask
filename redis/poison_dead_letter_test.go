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
	return b, calls.Load
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
