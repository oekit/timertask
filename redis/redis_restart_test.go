package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/timertask"
)

// TestRedisBackend_SecondStart_ErrBackendStopped 钉住 #124：Start 返回后
// stopChan 永久关闭且不重建，旧实现的二次 Start 是静默哑火——新拉起的消费
// goroutine 立刻命中已关闭的 stopChan 全部退出，Start 却阻塞在新的 ctx 上
// 看起来健康，本实例消费吞吐静默归零（多实例下任务由别的实例兜住、单实例
// 下等于停摆，且无任何信号）。修复后实例一次性：二次 Start 与停机后的 Send
// 都显式返回 timertask.ErrBackendStopped。
func TestRedisBackend_SecondStart_ErrBackendStopped(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	var ran atomic.Int32
	b.handler = func(ctx context.Context, task *timertask.Task) error {
		ran.Add(1)
		return nil
	}

	// 正对照：第一段 ctx 里任务正常执行
	ctx1, cancel1 := context.WithCancel(context.Background())
	startErr1 := make(chan error, 1)
	go func() { startErr1 <- b.Start(ctx1) }()
	if err := b.Send(context.Background(), &timertask.Task{ID: "c1", EndTime: time.Now().Unix()}); err != nil {
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
	case <-startErr1:
	case <-time.After(2 * time.Second):
		t.Fatal("Start(ctx1) 未返回")
	}

	// 停机后 Send 必须报错：实例的消费循环已退出，继续投递只会制造
	// 「写入成功、本实例零消费」的错觉
	if err := b.Send(context.Background(), &timertask.Task{ID: "after-stop", EndTime: time.Now().Unix()}); !errors.Is(err, timertask.ErrBackendStopped) {
		t.Fatalf("停机后 Send = %v, want ErrBackendStopped", err)
	}

	// 二次 Start 必须立刻明确失败，而不是拉起一组永不消费的僵尸 goroutine
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	startErr2 := make(chan error, 1)
	go func() { startErr2 <- b.Start(ctx2) }()
	select {
	case err := <-startErr2:
		if !errors.Is(err, timertask.ErrBackendStopped) {
			t.Fatalf("二次 Start = %v, want ErrBackendStopped", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("二次 Start 未返回：僵尸形态（本实例消费吞吐静默归零，#124）")
	}
}
