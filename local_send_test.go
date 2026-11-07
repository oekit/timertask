package timertask

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestLocalBackend_Send_DoesNotMutateCallerTask 钉住 #97：Send 的缺省补齐
// （CreateTime/MaxRetry）与 worker 的重试推进（RetryCount）都不得改写调用方
// 的 Task 对象——引擎内部持有并只改写自己的副本。
func TestLocalBackend_Send_DoesNotMutateCallerTask(t *testing.T) {
	var attempts atomic.Int32
	b := NewLocalBackend(func(ctx context.Context, task *Task) error {
		attempts.Add(1)
		return errors.New("always fail")
	}, WithWorkers(1), WithRetry(2, time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()

	caller := &Task{ID: "mut-1", EndTime: time.Now().Unix()}
	if err := b.Send(ctx, caller); err != nil {
		t.Fatal(err)
	}

	// 等 3 次尝试全部发生（1 次初始 + 2 次重试；重试会推进引擎副本的 RetryCount）
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() < 3 {
		time.Sleep(2 * time.Millisecond)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}

	if caller.CreateTime != 0 {
		t.Errorf("调用方 CreateTime 被引擎改写为 %d（#97）", caller.CreateTime)
	}
	if caller.MaxRetry != 0 {
		t.Errorf("调用方 MaxRetry 被引擎改写为 %d（#97）", caller.MaxRetry)
	}
	if caller.RetryCount != 0 {
		t.Errorf("调用方 RetryCount 被引擎改写为 %d（#97）", caller.RetryCount)
	}
}
