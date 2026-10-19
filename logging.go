package timertask

import (
	"log/slog"
)

// logger 是包内日志器。
//
// 根模块只用标准库 log/slog：本地形态要能在「纯 Go 嵌入式服务」里使用
// （CGO_ENABLED=0 静态链接、SQLite 落盘、零外部中间件），不能因为日志
// 就把 oekit/logx 拖进依赖面。Redis 形态不受此约束，仍用 logx
// （见 github.com/oekit/timertask/redis）。
var logger = slog.Default()

// goSafe 启动一个 panic 安全的后台 goroutine。
//
// 等价于 oekit/concurrency/async.GoSafe，但根模块不再依赖该仓——
// 根模块的 require 段只允许有 github.com/oekit/pkg（cron 解析）。
//
// panic 在 goroutine 出口被就地 recover 并记日志，不会打死 worker：
// async.GoSafe 的 recover 同样在出口，只是本包自己实现以保持依赖面最小。
func goSafe(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("goroutine panic recovered", "panic", r)
			}
		}()
		fn()
	}()
}
