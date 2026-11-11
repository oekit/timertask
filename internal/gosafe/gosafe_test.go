package gosafe

import (
	"testing"
	"time"
)

// TestGo_RecoversPanic 钉住 Go 的契约：panic 被就地 recover，不外泄
// （裸 goroutine 里的 panic 会打死整个进程）。
func TestGo_RecoversPanic(t *testing.T) {
	done := make(chan struct{})
	Go(func() {
		defer close(done)
		panic("boom")
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Go 未恢复 panic")
	}
}

// TestGo_RunsFunction Go 必须真的执行传入的函数。
func TestGo_RunsFunction(t *testing.T) {
	ran := make(chan struct{})
	Go(func() { close(ran) })
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("Go 未执行传入的函数")
	}
}

// TestGo_NormalReturn 正常返回路径同样要执行完并正常退出。
func TestGo_NormalReturn(t *testing.T) {
	done := make(chan struct{})
	Go(func() { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Go 未执行完传入的函数")
	}
}
