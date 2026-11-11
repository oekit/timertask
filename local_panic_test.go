package timertask

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pkgerrors "github.com/oekit/pkg/errors"
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

// TestLocalBackend_PanicError_KeepsErrorIdentity 钉住 #173：handler 以 panic
// 携带业务 error 时（上层用 panic 传错误的仓内惯例并不少见），收编结果必须
// 保留 error 身份（errors.Is/As 穿透判定）且携带发生时的 goroutine 栈——
// 此前 fmt.Errorf("handler panic: %v", r) 把身份拍平成纯文本，重试判定/
// 死信归因全部断链，排障只剩消息文本。收编单源为 pkg errors.Recovered
// （*PanicError 载体）。
func TestLocalBackend_PanicError_KeepsErrorIdentity(t *testing.T) {
	sentinel := errors.New("biz boom")
	var got error
	done := make(chan struct{})
	b := NewLocalBackend(func(ctx context.Context, task *Task) error {
		panic(sentinel) // 以 panic 携带业务 error
	},
		WithWorkers(1),
		WithRetry(0, time.Millisecond),
		WithOnFailure(func(task *Task, err error) {
			got = err
			close(done)
		}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = b.Start(ctx) }()
	if err := b.Send(ctx, &Task{
		ID:      "panic-identity-1",
		EndTime: time.Now().Unix(),
		Action:  "test",
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("onFailure 未被调用")
	}
	if got == nil {
		t.Fatal("onFailure 收到的 err 为 nil")
	}
	// error 身份不丢：errors.Is 穿透 PanicError 直达业务侧 panic 出去的原始错误
	if !errors.Is(got, sentinel) {
		t.Fatalf("err=%v, want errors.Is 判定到原始 sentinel（#173 身份断链）", got)
	}
	// 载体是 pkg errors 的 *PanicError：栈与类型信息恒在场
	var perr *pkgerrors.PanicError
	if !errors.As(got, &perr) {
		t.Fatalf("err=%v, want errors.As 判定到 *pkgerrors.PanicError", got)
	}
	if len(perr.Stack) == 0 {
		t.Error("PanicError.Stack 为空，排障无栈可用（#173）")
	}
	if !strings.Contains(perr.Error(), "timertask.bizError") && !strings.Contains(perr.Error(), "panic: *errors.errorString") {
		t.Logf("PanicError 文本（应含 panic 值类型信息）: %s", perr.Error())
	}
}
