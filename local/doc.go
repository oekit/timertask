// Package local 提供纯本地的 cron 调度器：按 cron 表达式触发任务。
//
// 零外部中间件、零 Redis 依赖——依赖面只有标准库与
// github.com/oekit/pkg/timex/cron（cron 表达式解析复用 oekit 矩阵的实现，
// 不重复造轮子）。适合纯 Go 嵌入式服务形态（CGO_ENABLED=0 静态链接、
// SQLite 落盘、零外部中间件）——这类形态用不了 Redis 后端，
// 但仍需要「到点自动干活」的能力。
//
// # 与根包的关系
//
// 本包是 github.com/oekit/timertask 的子包（同模块），但不 import 根包：
// 两者互不依赖（根包也不依赖本包），各自独立演进。需要「延迟队列」
// （投递到未来某时刻）而不是「cron 触发」时，用根包的 LocalBackend。
//
// # 语义（明确约定）
//
// 并发度：
//
//   - Config.Concurrency 是全局并发度（同时执行的任务数上限），默认 1（严格串行）。
//   - 同一 spec 的触发始终串行（自身永不重叠）：上一轮没跑完时，
//     新的触发进入该 spec 的待执行队列。
//
// 错过触发点（进程停机、时钟跳变、上一轮未跑完）：
//
//   - Config.CatchUp = CatchUpSkip（默认）：只执行最近的一个触发点，
//     其余错过点全部跳过；待执行队列里的旧触发会被新触发替换
//     （同一 spec 最多积压 1 个）——不会因为任务慢而无限堆积。
//   - Config.CatchUp = CatchUpAll：所有错过的触发点按时间顺序逐一补跑。
//
// 进程退出：
//
//   - Stop 不打断正在执行的任务：任务的 ctx 不会被取消，Stop 等待它们结束；
//     Stop 的等待上限由调用方给的 ctx 决定，超时返回 ctx.Err()，
//     任务继续跑完（Go 无法安全中断 goroutine）。
//   - Start 的 ctx 被取消时：调度立刻停止，任务的 ctx 同时被取消
//     （任务可自行感知并尽快退出），未开始的触发被丢弃。
//   - 两种情况下「未开始的触发」都会被丢弃——本地调度器不持久化。
//
// 重复注册：
//
//   - 同一个 spec 重复 Schedule 返回 ErrDuplicateSpec（不静默覆盖：
//     覆盖会悄悄丢掉原处理函数）。要换处理函数请先 Unschedule。
//
// # 示例
//
//	sched, err := local.New(local.Config{Concurrency: 2})
//	if err != nil {
//		return err
//	}
//	if err := sched.Schedule("0 9 * * 1-5", func(ctx context.Context, at time.Time) error {
//		return report(ctx)
//	}); err != nil {
//		return err
//	}
//	go func() { _ = sched.Start(ctx) }()
//	defer sched.Stop(context.Background())
package local
