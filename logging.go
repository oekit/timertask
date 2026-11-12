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

// ErrKey 是错误字段的日志键，与 logx.ErrKey（oekit/logx）同值 "err"。
//
// #193 口径：全矩阵硬规则「错误字段统一 logx.ErrKey、禁手写 "err" 字面量」
// 在本仓的落地面——根模块（含 local/cron 子包）按 logging.go 的依赖政策
// 不引 logx，错误字段键以本常量单源；redis 嵌套模块已依赖 logx，直接用
// logx.ErrKey。两个常量的值一致性由 redis 模块内的对照测试钉死
// （member_stability_test.go 旁的 errkey_parity_test.go），键值漂移即测试红。
const ErrKey = "err"
