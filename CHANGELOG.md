# Changelog — oekit/timertask

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [v0.1.8] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4

## [v0.1.7] — 2026-10-17

- fix(reliable): reapLoop/requeueLeasesOnShutdown 接入 Start/Stop 生命周期——v0.1.5 起租约回扫从未被启动，at-least-once 模式崩溃实例的未 Ack 任务实际不会被捡回（回扫仅在测试中被手动调用）；停机路径补同步兜底回扫；测试消 govet shadow（CI Lint 门禁红）

## [Unreleased]

- **feat(redis)**：可选 **at-least-once 可靠投递**（`WithAtLeastOnce(vis)` /
  `WithDeliveryMode(DeliveryAtLeastOnce)`）。claim 时 ZSet 条目移入
  `{timertask.<buz>}.processing` 租约集，handler 成功 Ack 删除；租约超时由
  回扫按 RetryCount 重投，kill -9 后未 Ack 任务不丢。默认仍为 at-most-once，
  行为与 v0.1.4 一致。文档补「哪种任务选哪种模式」
  （[#2](https://github.com/oekit/timertask/issues/2)）

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/timertask`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。