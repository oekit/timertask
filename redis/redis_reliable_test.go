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

func newReliableBackend(t *testing.T, vis time.Duration, handler timertask.Handler) *RedisBackend {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisBackend("rel", client, handler, timertask.WithAtLeastOnce(vis))
}

// TestAtLeastOnce_AckOnSuccess 验证成功后租约被释放（processing 清空）。
func TestAtLeastOnce_AckOnSuccess(t *testing.T) {
	done := make(chan struct{})
	b := newReliableBackend(t, 30*time.Second, func(ctx context.Context, task *timertask.Task) error {
		close(done)
		return nil
	})
	task := &timertask.Task{ID: "ack-1", EndTime: time.Now().Unix() - 1, MaxRetry: 0}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	// 等 Ack
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		n, _ := b.client.ZCard(ctx, b.processingKey()).Result()
		if n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	n, _ := b.client.ZCard(ctx, b.processingKey()).Result()
	t.Fatalf("processing lease not acked, zcard=%d", n)
}

// TestAtLeastOnce_LeaseExpireRequeue 验证租约超时后任务被重投（kill -9 等价路径）。
func TestAtLeastOnce_LeaseExpireRequeue(t *testing.T) {
	var calls atomic.Int32
	b := newReliableBackend(t, 1*time.Second, func(ctx context.Context, task *timertask.Task) error {
		calls.Add(1)
		return nil
	})
	task := &timertask.Task{ID: "lease-1", EndTime: time.Now().Unix() - 1, MaxRetry: 5}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	// 直接 claim 进租约集（模拟 worker 拿到后进程崩溃，未 Ack）。
	// member 从 ZSet 读真实条目——与生产一致（member 由 Send 规范化写入，
	// 测试不可自造逐字节副本，CreateTime 钉死即对不上）
	member, err := b.client.ZRange(context.Background(), b.taskKey(task.ID), 0, 0).Result()
	if err != nil || len(member) != 1 {
		t.Fatalf("read member: %v (%d)", err, len(member))
	}
	ok, err := b.claimTaskWithLease(context.Background(), b.taskKey(task.ID), member[0], time.Now().Unix())
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	// 手动把租约拨成已过期，触发回扫重投
	past := time.Now().Unix() - 10
	if err = b.client.ZAdd(context.Background(), b.processingKey(), goredis.Z{
		Score:  float64(past),
		Member: member[0],
	}).Err(); err != nil {
		t.Fatal(err)
	}

	b.reapExpiredLeases(context.Background())

	// 应重投回分片 ZSet，且 processing 已清
	pending, err := b.PendingCount(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("pending=%d, want 1 (requeued)", pending)
	}
	n, _ := b.client.ZCard(context.Background(), b.processingKey()).Result()
	if n != 0 {
		t.Fatalf("processing zcard=%d, want 0", n)
	}
}

// TestAtMostOnce_DefaultUnchanged 默认路径仍为 claim 即删。
func TestAtMostOnce_DefaultUnchanged(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	b := NewRedisBackend("amo", client, func(ctx context.Context, task *timertask.Task) error {
		return nil
	}, timertask.WithBuckets(4))
	// 不启 Start，手动把全部分片划给本实例
	for i := 0; i < 4; i++ {
		b.shardManager.myShards = append(b.shardManager.myShards, i)
	}
	task := &timertask.Task{ID: "amo-1", EndTime: time.Now().Unix() - 1, MaxRetry: 0}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	// consumeRemoteOnce 应 deleteScript 删除，不进 processing
	b.consumeRemoteOnce(context.Background())
	n, _ := client.ZCard(context.Background(), b.processingKey()).Result()
	if n != 0 {
		t.Fatalf("at-most-once 不应写入 processing, zcard=%d", n)
	}
	pending, _ := b.PendingCount(context.Background())
	if pending != 0 {
		t.Fatalf("pending=%d, want 0 after claim", pending)
	}
	if b.LocalPendingCount() != 1 {
		t.Fatalf("local=%d, want 1", b.LocalPendingCount())
	}
}

// leaseScore 轮询 processing 集中唯一 member 的 score，超过 min 即返回
// （min=0 表示等首次出现；min>0 表示等续租把它推高）。
func leaseScore(t *testing.T, b *RedisBackend, min float64) float64 {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		members, err := b.client.ZRangeWithScores(context.Background(), b.processingKey(), 0, -1).Result()
		if err == nil && len(members) == 1 && members[0].Score > min {
			return members[0].Score
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("processing 租约 score 未在 3s 内超过 %v", min)
	return 0
}

// TestAtLeastOnce_LeaseRenewedDuringSlowHandler 钉住 #93/#95：handler 执行
// 期间必须有租约续期——跑得比 visibilityTimeout 慢的 handler 不被回扫误判
// 死亡而重投。此前 claim 后租约永不续期，慢 handler 到点必被重投、至少执行
// 两次（必现重复，非偶发），首次成功后的 Ack 拦不住已重投的副本。
func TestAtLeastOnce_LeaseRenewedDuringSlowHandler(t *testing.T) {
	vis := 600 * time.Millisecond // 租约实际按 1s 计（亚秒配置的补偿规则）
	release := make(chan struct{})
	var calls atomic.Int32
	b := newReliableBackend(t, vis, func(ctx context.Context, task *timertask.Task) error {
		calls.Add(1)
		<-release // 模拟慢 handler：阻塞到测试放行
		return nil
	})
	task := &timertask.Task{ID: "slow-1", EndTime: time.Now().Unix() - 1, MaxRetry: 5}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()

	// 等任务被 claim 进 processing，记下初始租约 deadline
	initial := leaseScore(t, b, 0)
	// 等续租发生（deadline 被推高——证明 renewal 在跑）
	leaseScore(t, b, initial)
	// 走过初始 deadline：此刻若无续期，租约已过期
	if wait := time.Until(time.Unix(int64(initial), 0).Add(200 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}

	// handler 仍在执行（未 Ack）：回扫不得把它重投（旧实现此处必重投）
	b.reapExpiredLeases(context.Background())
	if pending, _ := b.PendingCount(context.Background()); pending != 0 {
		t.Fatalf("慢 handler 执行中租约被回扫重投（#93/#95），pending = %d", pending)
	}

	close(release) // 放行 handler → 成功 → Ack
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n, _ := b.client.ZCard(context.Background(), b.processingKey()).Result()
		if n == 0 {
			if got := calls.Load(); got != 1 {
				t.Fatalf("handler 执行 %d 次，want 1（无重复投递）", got)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("handler 成功后租约未被 Ack（processing 仍有条目）")
}
