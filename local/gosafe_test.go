package local

import (
	"context"
	"testing"
	"time"
)

// TestScheduler_GoSafe_RecoversPanic 钉住 goSafe 的契约：panic 被就地 recover，
// 不外泄。
//
// 背景：调度器此前用**裸 `go`** 启动内部 goroutine（`go s.runEntry(e)` /
// `go s.run(ctx)`）。裸 goroutine 里的 panic **会打死整个进程**（不是只死那
// 一个 goroutine）。
//
// 注意区分两类 panic：
//   - **用户任务**的 panic：此前已由 Scheduler.invoke 就地收编为错误
//     （既有行为，见 scheduler_test.go 的 TestScheduler_TaskPanicDoesNotKillScheduler）；
//   - **调度器自身**的 panic（cron 计算、触发点记账等内部环节）：此前**没有**
//     任何保护 ⇒ 一次内部 panic 打死整个进程。本 PR 用 goSafe 覆盖这一类。
func TestScheduler_GoSafe_RecoversPanic(t *testing.T) {
	done := make(chan struct{})
	goSafe(func() {
		defer close(done)
		panic("boom")
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goSafe 未恢复 panic")
	}
}

// TestScheduler_GoSafe_RunsFunction goSafe 必须**真的执行**传入的函数。
func TestScheduler_GoSafe_RunsFunction(t *testing.T) {
	ran := make(chan struct{})
	goSafe(func() { close(ran) })
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("goSafe 未执行传入的函数")
	}
}

// TestScheduler_GoSafe_NormalReturn 正常返回路径同样要执行完并正常退出。
func TestScheduler_GoSafe_NormalReturn(t *testing.T) {
	done := make(chan struct{})
	goSafe(func() { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goSafe 未执行完传入的函数")
	}
	_ = context.Background()
}
