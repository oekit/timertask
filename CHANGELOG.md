# Changelog — oekit/timertask

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [redis/v0.2.0] — 2026-10-19

- feat(redis): Redis 后端（分片 + 可靠投递）自根包迁入嵌套模块 `github.com/oekit/timertask/redis`
  ——根模块依赖面剥离 go-redis，Redis 形态独立成模块按需引入

## [v0.2.1] — 2026-10-20

### docs：修正 v0.2.0 的迁移措辞——「import 路径替换即可」不准确（#19）

- v0.2.0 的 Breaking 段写「改用 `github.com/oekit/timertask/redis`（import 路径替换即可，API 不变）」
  ——**这句是错的**：`timertask/redis` 不是根包下的子目录，它是**独立的 Go module**
  （有自己的 go.mod、自己的 tag `redis/v0.2.0`）。消费方只改 import 路径、**不新增 require 行**，
  会直接构建失败（`no required module provides package github.com/oekit/timertask/redis`）
- 正确的迁移步骤（**新增 require 是必需的一步**）：

  ```bash
  go get github.com/oekit/timertask/redis@v0.2.0
  ```

  ```go
  // 旧
  backend := timertask.NewRedisBackend(timertask.RedisConfig{...})
  // 新（import 路径 + go.mod require 两处都要改）
  backend := redis.New(redis.RedisConfig{...})
  ```

- 仅文档修正，无代码变更

## [v0.2.0] — 2026-10-19

### Breaking
- **Redis 后端迁至嵌套模块**：`RedisBackend`/`NewRedisBackend`/`ShardManager`/`NewShardManager`
  从根包移除，改用 `github.com/oekit/timertask/redis`（**API 不变，但 import 路径与 go.mod
  require 两处都要改**——`timertask/redis` 是**独立 module**，需 `go get
  github.com/oekit/timertask/redis@v0.2.0` 新增 require 行；详见 v0.2.1 的迁移步骤）；
  根包保留 `Task`/`Handler`/全部 `With*` 选项，既有调用点一行不动
- **根包不再依赖 `github.com/oekit/logx`**：日志改用标准库 `log/slog`（消费方经
  `slog.SetDefault` 汇流）；同时剥离 `github.com/oekit/concurrency`（`async.GoSafe` 改为包内等价实现）

### feat(local)：cron 调度器子包
- `local.New(cfg)` / `Schedule(spec, fn)` / `Unschedule(spec)` / `Start(ctx)` / `Stop(ctx)`——
  按 cron 表达式触发任务，表达式解析复用 `github.com/oekit/pkg/timex/cron`（不另写一份 cron）
- 调度语义明确：并发度 `Config.Concurrency`（缺省 1 = 严格串行，同 spec 恒串行）；
  错过触发点 `CatchUpSkip`（缺省，只补最近一个）/ `CatchUpAll`（逐个补，`MaxCatchUp` 封顶）；
  `Stop(ctx)` **不打断**正在跑的任务并等待其结束（受调用方 ctx 约束），取消 `Start` 的 ctx
  则停止调度并取消任务 ctx；未开始的触发点丢弃
- 重复 `Schedule` 返回 `ErrDuplicateSpec`（不静默覆盖）
- 假时钟驱动：`local.FakeClock` + `Advance`，触发点序列可断言、不依赖 sleep

### build
- 根模块依赖面：go.mod require 仅 `github.com/oekit/pkg`——`import timertask` 与
  `import timertask/local` 的模块依赖面均不含 redis（纯 stdlib 嵌入式场景可用）

## [v0.1.10] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：concurrency v0.5.4 → v0.17.0、logx v0.6.0 → v0.7.0

## [v0.1.9] — 2026-10-17

- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [v0.1.8] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4

## [v0.1.7] — 2026-10-17

- fix(reliable): reapLoop/requeueLeasesOnShutdown 接入 Start/Stop 生命周期——v0.1.5 起租约回扫从未被启动，at-least-once 模式崩溃实例的未 Ack 任务实际不会被捡回（回扫仅在测试中被手动调用）；停机路径补同步兜底回扫；测试消 govet shadow（CI Lint 门禁红）

## [v0.1.5] — 2026-10-17

- **feat(redis)**：可选 **at-least-once 可靠投递**（`WithAtLeastOnce(vis)` /
  `WithDeliveryMode(DeliveryAtLeastOnce)`）。claim 时 ZSet 条目移入
  `{timertask.<buz>}.processing` 租约集，handler 成功 Ack 删除；租约超时由
  回扫按 RetryCount 重投，kill -9 后未 Ack 任务不丢。默认仍为 at-most-once，
  行为与 v0.1.4 一致。文档补「哪种任务选哪种模式」
  （[#2](https://github.com/oekit/timertask/issues/2)）

## [v0.1.4] — 2026-10-15

- build: 最低 Go 版本降至 1.24（go-redis v9.23.0 → v9.22.0、x/sys v0.48.0 → v0.41.0 锁兼容版）；内部依赖地板 concurrency v0.1.3 → v0.1.5、logx v0.1.6 → v0.1.9

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/timertask`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。