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
//
// 后台 goroutine 的 panic 安全启动（goSafe）已收敛到模块内单源
// internal/gosafe（#168，根包/local/cron 三份拷贝合一）。
var logger = slog.Default()
