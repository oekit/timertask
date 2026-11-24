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

// TestLocalBackend_SendRejectedDuringShutdownWindow 钉住 #292：Start 收到停机
// 信号后，scanLoop 已经退出，但 DrainWorkers 还要等 in-flight worker 到任务
// 边界（最长 DefaultShutdownDrainGrace）。此前 Send 只检查 st.stopped（Start
// 返回后才登记）⇒ 窗口内 Send 返回 nil、任务被压进内存堆，而停机路径只排水
// localQueue、从不处理堆内任务——调用方拿到「提交成功」，任务永久丢失。
//
// 修复：stopping 状态与 close(stopChan) 同一临界段登记，窗口内 Send 立即
// 返回 ErrBackendStopped，任务不入堆。
func TestLocalBackend_SendRejectedDuringShutdownWindow(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	b := NewLocalBackend(func(ctx context.Context, task *Task) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release // 卡住 in-flight worker，把 Start 停在排水窗口内
		return nil
	}, WithWorkers(1))

	ctx, cancel := context.WithCancel(context.Background())
	startErr := make(chan error, 1)
	go func() { startErr <- b.Start(ctx) }()
	defer func() {
		close(release)
		select {
		case <-startErr:
		case <-time.After(3 * time.Second):
			t.Error("Start 未返回")
		}
	}()

	if err := b.Send(context.Background(), &Task{ID: "inflight", EndTime: time.Now().Unix()}); err != nil {
		t.Fatalf("Send(inflight): %v", err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler 未进入（正对照失败）")
	}

	cancel()
	// stopChan 关闭 = 停机窗口已进入（stopping 状态在同一临界段登记）：
	// Start 此时仍阻塞在 DrainWorkers 里、尚未返回。
	select {
	case <-b.stopChan:
	case <-time.After(3 * time.Second):
		t.Fatal("停机窗口未进入")
	}

	if err := b.Send(context.Background(), &Task{ID: "late", EndTime: time.Now().Unix()}); !errors.Is(err, ErrBackendStopped) {
		t.Fatalf("停机窗口内 Send = %v, want ErrBackendStopped（#292：假成功会永久丢任务）", err)
	}
	// 被拒绝的任务不得入堆：PendingCount 不能出现「积压但无人消费」的假象。
	if n, err := b.PendingCount(context.Background()); err != nil || n != 0 {
		t.Fatalf("PendingCount = %d, %v; want 0（被拒绝的任务不入堆）", n, err)
	}
}
