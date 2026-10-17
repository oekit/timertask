package timertask

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newReliableBackend(t *testing.T, vis time.Duration, handler Handler) *RedisBackend {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisBackend("rel", client, handler, WithAtLeastOnce(vis))
}

// TestAtLeastOnce_AckOnSuccess 验证成功后租约被释放（processing 清空）。
func TestAtLeastOnce_AckOnSuccess(t *testing.T) {
	done := make(chan struct{})
	b := newReliableBackend(t, 30*time.Second, func(ctx context.Context, task *Task) error {
		close(done)
		return nil
	})
	task := &Task{ID: "ack-1", EndTime: time.Now().Unix() - 1, MaxRetry: 0}
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
	b := newReliableBackend(t, 1*time.Second, func(ctx context.Context, task *Task) error {
		calls.Add(1)
		return nil
	})
	task := &Task{ID: "lease-1", EndTime: time.Now().Unix() - 1, MaxRetry: 5}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	// 直接 claim 进租约集（模拟 worker 拿到后进程崩溃，未 Ack）
	ok, err := b.claimTaskWithLease(context.Background(), b.taskKey(task.ID), mustJSON(task), time.Now().Unix())
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	// 手动把租约拨成已过期，触发回扫重投
	past := time.Now().Unix() - 10
	if err := b.client.ZAdd(context.Background(), b.processingKey(), redis.Z{
		Score:  float64(past),
		Member: mustJSON(task),
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
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	b := NewRedisBackend("amo", client, func(ctx context.Context, task *Task) error {
		return nil
	}, WithBuckets(4))
	// 不启 Start，手动把全部分片划给本实例
	for i := 0; i < 4; i++ {
		b.shardManager.myShards = append(b.shardManager.myShards, i)
	}
	task := &Task{ID: "amo-1", EndTime: time.Now().Unix() - 1, MaxRetry: 0}
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
