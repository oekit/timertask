// Package gosafe 是本模块（根包 timertask、local、cron 三包）goSafe 的单源
// 实现（#168：此前同模块三份拷贝——logging.go / local/gosafe.go /
// cron/gosafe.go，local 与 cron 两份逐字相同，根包版仅少 stack 字段）。
//
// local/gosafe.go 的「本包刻意不 import 根包（两者互不依赖）」理由在
// #78（local.Clock 别名归一到根包定义）之后已失实——local 经 clock.go
// import 根包，cron 经 local 传递可达。三份收敛到本 internal 包（不出模块
// API 面），日志口径随之统一：panic 值 + goroutine 栈双字段恒在场。
//
// 为什么不用 oekit/concurrency/async.GoSafe：根模块的依赖面刻意只有
// github.com/oekit/pkg（纯 stdlib 嵌入式场景可用，见根包 logging.go），
// 不为一个 recover 拖进 concurrency 仓；redis 嵌套模块不受此约束，
// 仍用 async.GoSafe。
package gosafe

import (
	"log/slog"
	"runtime/debug"
)

// Go 启动一个 panic 安全的后台 goroutine。
//
// 等价于 oekit/concurrency/async.GoSafe。为什么必须 recover：裸 `go` 启动的
// goroutine 里 panic **会打死整个进程**（不是只死那一个 goroutine）。本模块
// 的后台 goroutine 体里既有调度器自身环节也有用户任务，都属于「单个任务
// 出错」，不该拖垮整个进程——panic 在 goroutine 出口被就地 recover 并记
// 日志（panic 值 + 栈双字段）。
func Go(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Default().Error("goroutine panic recovered", "panic", r, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
}
