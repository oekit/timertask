package local

import (
	"log/slog"
	"runtime/debug"
)

// goSafe 启动一个 panic 安全的后台 goroutine。
//
// 等价于 oekit/concurrency/async.GoSafe 与根包 timertask 的同名内部函数，
// 但本包**刻意不 import 根包**（两者互不依赖，各自独立演进，见本包 doc
// 的「与根包的关系」），也不依赖 concurrency 仓——因此在这里自带一份最小
// 实现，把依赖面保持在「标准库 + oekit/pkg/timex/cron」。
//
// 为什么必须 recover：裸 `go` 启动的 goroutine 里 panic **会打死整个进程**
// （不是只死那一个 goroutine）。本包的触发任务由调用方提供，panic 属���
// 「某个用户的任务出错」，不该拖垮整个调度器进程。
func goSafe(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Default().Error("goroutine panic recovered", "panic", r, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
}
