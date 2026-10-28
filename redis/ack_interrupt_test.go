package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// TestAtLeastOnce_InterruptDuringRetry_KeepsLease 验证「重试窗口内被中断」时
// 租约不被释放——任务已从分片 ZSet claim 走，租约是它唯一的去处：
// 释放了它，任务就落到「哪都不是」的地方（不在分片 ZSet、不在 processing、
// 不在死信 ZSet），回扫租约也捞不回来 ⇒ at-least-once 承诺失效、任务永久丢失。
//
// 修复前：defer 里的 Ack 在**每条**返回路径上都执行（含重试窗口内的
// ctx.Done / stopChan 中断），与同处注释「仅 handler 仍在重试窗口内不 Ack」矛盾。
func TestAtLeastOnce_InterruptDuringRetry_KeepsLease(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	entered := make(chan struct{}, 1)
	b := NewRedisBackend("rel", client, func(ctx context.Context, task *timertask.Task) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		return errors.New("handler always fails")
	},
		timertask.WithAtLeastOnce(30*time.Second),
		timertask.WithRetry(5, time.Hour), // 停在重试窗口里，便于精确中断
	)

	task := &timertask.Task{ID: "interrupt-1", EndTime: time.Now().Unix() - 1, MaxRetry: 5}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Start(ctx) }()

	// 等 handler 第一次失败、进入重试等待
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called")
	}
	time.Sleep(200 * time.Millisecond) // 确保已进重试窗口

	// 中断：ctx 取消 → processTask 在重试窗口内 return
	cancel()
	time.Sleep(500 * time.Millisecond) // 等 defer 里的 Ack 跑完

	pkey := b.processingKey()
	leased, err := client.ZCard(context.Background(), pkey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if leased == 0 {
		t.Fatalf("租约被释放了：重试窗口内被中断的任务既不在分片 ZSet 也不在 processing（zcard=%d）⇒ 任务永久丢失，at-least-once 失效", leased)
	}
}

// TestAtLeastOnce_InterruptDuringRetry_TaskComesBack 验证中断的任务不只是
// 「租约还在」，而是真的会被回扫捡回重投（租约到期 → reapExpiredLeases）。
//
// 这是 at-least-once 的闭环：中断 ⇒ 租约保留 ⇒ 回扫重投 ⇒ 任务再被执行。
func TestAtLeastOnce_InterruptDuringRetry_TaskComesBack(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	entered := make(chan struct{}, 1)
	b := NewRedisBackend("rel", client, func(ctx context.Context, task *timertask.Task) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		return errors.New("handler always fails")
	},
		timertask.WithAtLeastOnce(1*time.Second), // 租约 1s 后到期
		timertask.WithRetry(5, time.Hour),
	)

	task := &timertask.Task{ID: "interrupt-2", EndTime: time.Now().Unix() - 1, MaxRetry: 5}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Start(ctx) }()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called")
	}
	time.Sleep(200 * time.Millisecond)
	cancel() // 中断：停在重试窗口里
	time.Sleep(300 * time.Millisecond)

	// 等租约到期（visibilityTimeout=1s，从 claim 起算），再由回扫捡回重投
	// ⇒ 任务回到分片 ZSet（可被再次消费）。
	time.Sleep(1500 * time.Millisecond)
	b.reapExpiredLeases(context.Background())

	n, err := client.ZCard(context.Background(), b.processingKey()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("回扫后租约集应已清空，zcard=%d", n)
	}
	requeued, err := client.ZCard(context.Background(), b.taskKey("interrupt-2")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if requeued == 0 {
		t.Fatal("中断的任务没有被回扫重投回分片 ZSet：at-least-once 闭环断裂")
	}
}
