package timertask

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestLocalBackend_HandlerPanic_TreatedAsFailure 回归：processTask 裸调
// handler 时，一次 panic 会打死 worker（async.GoSafe 的 recover 在 goroutine
// 出口），N 个 panic 任务耗尽 worker 池后整个后端瘫痪。钉住：panic 就地
// 收编为任务失败，走重试且 worker 存活。
func TestLocalBackend_HandlerPanic_TreatedAsFailure(t *testing.T) {
	var attempts atomic.Int32
	handler := func(ctx context.Context, task *Task) error {
		if attempts.Add(1) == 1 {
			panic("handler boom")
		}
		return nil // 重试成功
	}
	b := NewLocalBackend(handler,
		WithWorkers(1),
		WithRetry(1, 10*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = b.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)

	if err := b.Send(ctx, &Task{
		ID:       "panic-1",
		EndTime:  time.Now().Unix(),
		Action:   "test",
		MaxRetry: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// 第一次 panic 后应重试并成功
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected retry after handler panic, got %d attempts", got)
	}
}
