package cron

import (
	"log/slog"
	"runtime/debug"
)

// goSafe 启动一个 panic 安全的后台 goroutine。
//
// 等价于 oekit/concurrency/async.GoSafe 与根包/local 包的同名内部函数，
// 但本包沿用仓内先例自带最小实现（local/gosafe.go 同款理由）：本包是
// github.com/oekit/timertask 的同模块子包，依赖面保持「标准库 +
// oekit/pkg/timex/cron」，不为一个 recover 拖进 concurrency 仓——
// 禁止裸 `go func` 的纪律照常执行（panic 不得打死触发引擎）。
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
