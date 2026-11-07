package timertask

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestLocalBackend_ScanOnce_FullQueueDoesNotBlockSend 钉住 #92/#94：scanOnce
// 不得持 b.mu 阻塞在满 localQueue 的发送上——worker 全卡在慢 handler、
// localQueue（容量 10000）涨满时，持锁挂起的 scanOnce 会把 Send/PendingCount
// 全部堵在 b.mu 上：加锁互斥量被无限期占住，不只是延迟，是整体卡死。
//
// 复现：不启 worker（等价于 worker 全卡死），预填满 localQueue，提交一个到期
// 任务并触发 scanOnce——旧实现持锁挂在发送上，b.mu 永不释放。
func TestLocalBackend_ScanOnce_FullQueueDoesNotBlockSend(t *testing.T) {
	b := NewLocalBackend(func(ctx context.Context, task *Task) error { return nil })

	// 预填满 localQueue：scanOnce 的发送必然阻塞
	for i := 0; i < cap(b.localQueue); i++ {
		b.localQueue <- &Task{ID: fmt.Sprintf("filler-%d", i)}
	}

	if err := b.Send(context.Background(), &Task{ID: "due-1", EndTime: time.Now().Unix() - 1}); err != nil {
		t.Fatal(err)
	}

	scanDone := make(chan struct{})
	go func() {
		b.scanOnce(context.Background())
		close(scanDone)
	}()

	// 等待「锁内 pop 完成」（taskMap 清空）：此后 scanOnce 处于发送阻塞态。
	// 旧实现持锁阻塞 ⇒ 本轮询拿不到锁 ⇒ 超时快速失败（不会挂死整个测试）。
	popped := make(chan struct{})
	go func() {
		for {
			b.mu.Lock()
			empty := len(b.taskMap) == 0
			b.mu.Unlock()
			if empty {
				close(popped)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	select {
	case <-popped:
	case <-time.After(2 * time.Second):
		t.Fatal("scanOnce 未在 2s 内弹出到期任务（疑似持锁阻塞，#92/#94）")
	}

	// 核心断言：scanOnce 阻塞在满队列发送上时，Send 不被堵死
	sendDone := make(chan struct{})
	go func() {
		_ = b.Send(context.Background(), &Task{ID: "probe-1", EndTime: time.Now().Unix() + 100})
		close(sendDone)
	}()
	select {
	case <-sendDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Send 被 scanOnce 持锁阻塞（#92/#94 回归）")
	}

	// PendingCount 同样不被堵死
	pendDone := make(chan int64, 1)
	go func() {
		n, _ := b.PendingCount(context.Background())
		pendDone <- n
	}()
	select {
	case n := <-pendDone:
		if n != 1 { // due-1 已弹出，probe-1 在队
			t.Errorf("PendingCount = %d, want 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PendingCount 被 scanOnce 持锁阻塞（#92/#94 回归）")
	}

	// 释放一个槽位：scanOnce 的发送应完成并返回
	<-b.localQueue
	select {
	case <-scanDone:
	case <-time.After(2 * time.Second):
		t.Fatal("槽位释放后 scanOnce 未完成发送")
	}
}

// TestLocalBackend_SendDuplicateID_CoversInFlightWindow 钉住 #98：同 ID 去重
// 须覆盖执行窗口——scanOnce 弹出后、processTask 返回前，同 ID Send 不入队；
// 处理结束后 ID 释放、可重新提交。全程同步驱动，不依赖 sleep。
func TestLocalBackend_SendDuplicateID_CoversInFlightWindow(t *testing.T) {
	var calls int
	b := NewLocalBackend(func(ctx context.Context, task *Task) error {
		calls++
		return nil
	}, WithWorkers(1))

	ctx := context.Background()
	if err := b.Send(ctx, &Task{ID: "dup-win-1", EndTime: time.Now().Unix() - 1}); err != nil {
		t.Fatal(err)
	}
	b.scanOnce(ctx) // 弹出 → inflight 占用 + 送入 localQueue

	// 执行窗口内（已弹出、未处理）重复提交：不入队
	if err := b.Send(ctx, &Task{ID: "dup-win-1", EndTime: time.Now().Unix() - 1}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.PendingCount(ctx); n != 0 {
		t.Fatalf("执行窗口内同 ID Send 不应入队（#98），PendingCount = %d", n)
	}

	// 模拟 worker 取走并处理完成 → inflight 释放
	got := <-b.localQueue
	if got.ID != "dup-win-1" {
		t.Fatalf("localQueue 取到 %q，want dup-win-1", got.ID)
	}
	b.processTask(ctx, got)
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}

	// 处理结束后：同 ID 可重新提交
	if err := b.Send(ctx, &Task{ID: "dup-win-1", EndTime: time.Now().Unix() - 1}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.PendingCount(ctx); n != 1 {
		t.Fatalf("处理结束后同 ID 应可重新提交，PendingCount = %d", n)
	}
}
