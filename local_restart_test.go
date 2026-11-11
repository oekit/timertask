package timertask

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestLocalBackend_SecondStart_ErrBackendStopped 钉住 #124：Start 返回后
// stopChan 永久关闭且不重建，旧实现的二次 Start 是静默哑火——新拉起的
// scanLoop/workerLoop 立刻命中已关闭的 stopChan 全部退出，Start 却阻塞在
// 新的 ctx 上看起来健康，Send 照常入堆但零消费（「写入成功、无人消费、
// 无任何信号」，比 panic 更难排查）。修复后实例一次性：二次 Start 与停机
// 后的 Send 都显式返回 ErrBackendStopped。
func TestLocalBackend_SecondStart_ErrBackendStopped(t *testing.T) {
	var ran atomic.Int32
	b := NewLocalBackend(func(ctx context.Context, task *Task) error {
		ran.Add(1)
		return nil
	}, WithWorkers(1))

	// 正对照：第一段 ctx 里任务正常执行
	ctx1, cancel1 := context.WithCancel(context.Background())
	startErr1 := make(chan error, 1)
	go func() { startErr1 <- b.Start(ctx1) }()
	if err := b.Send(context.Background(), &Task{ID: "c1", EndTime: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ran.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if ran.Load() == 0 {
		t.Fatal("handler 未执行（正对照失败）")
	}
	cancel1()
	select {
	case err := <-startErr1:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start(ctx1) = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start(ctx1) 未返回")
	}

	// 停机后 Send 必须报错，而不是继续入堆（PendingCount 增长会误导运维
	// 以为队列只是积压）
	if err := b.Send(context.Background(), &Task{ID: "after-stop", EndTime: time.Now().Add(time.Hour).Unix()}); !errors.Is(err, ErrBackendStopped) {
		t.Fatalf("停机后 Send = %v, want ErrBackendStopped", err)
	}
	if n, _ := b.PendingCount(context.Background()); n != 0 {
		t.Fatalf("停机后任务仍入堆：PendingCount = %d, want 0", n)
	}

	// 二次 Start 必须立刻明确失败，而不是拉起一组永不消费的僵尸 goroutine
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	startErr2 := make(chan error, 1)
	go func() { startErr2 <- b.Start(ctx2) }()
	select {
	case err := <-startErr2:
		if !errors.Is(err, ErrBackendStopped) {
			t.Fatalf("二次 Start = %v, want ErrBackendStopped", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("二次 Start 未返回：僵尸形态（stopChan 已关、消费者全部哑火，Start 却看起来健康，#124）")
	}
}

// TestLocalBackend_ConcurrentStart_Rejected 钉住 #124 的并发形态：首个 Start
// 尚未返回时再调 Start 也必须显式报错——否则第二组 consumer 会在第一段
// 停机时（stopChan 关闭）集体哑火，调用方却阻塞在第二个 ctx 上毫无感知。
func TestLocalBackend_ConcurrentStart_Rejected(t *testing.T) {
	b := NewLocalBackend(func(ctx context.Context, task *Task) error { return nil }, WithWorkers(1))
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	go func() { _ = b.Start(ctx1) }()

	// 确认第一个 Start 已完成状态登记（started 在 Start 入口同步登记）
	deadline := time.Now().Add(2 * time.Second)
	for {
		b.mu.Lock()
		started := b.started
		b.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("第一个 Start 未进入 started 状态")
		}
		time.Sleep(time.Millisecond)
	}

	err := b.Start(context.Background())
	if err == nil || errors.Is(err, ErrBackendStopped) || !errors.Is(err, ErrBackendAlreadyStarted) {
		t.Fatalf("并发二次 Start = %v, want ErrBackendAlreadyStarted", err)
	}
}
