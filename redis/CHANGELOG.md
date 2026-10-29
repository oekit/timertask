# Changelog — oekit/timertask/redis

由 oekit/timertask v0.2.0 拆分而来（Redis 形态独立成模块）。

## [v0.2.5] — 2026-10-29

- 跟随 timertask v0.2.5 发版（release_train 嵌套 train）

## [v0.2.4] — 2026-10-28

- 跟随 timertask v0.2.4 发版（release_train 嵌套 train）

## [v0.2.3] — 2026-10-28

- fix: 分片下标 `int(hash) % len(clients)` 在 **32 位平台**上 uint32→int 截断成负数 ⇒ 负下标越界 panic，分片分配 goroutine 当场死亡（心跳门控还关着 ⇒ 实例静默不消费任务）。修法：先在 uint32 空间取模再转 int
- fix: 死信写入与终态 Ack 此前用 `context.Background()` 调 Redis ⇒ Redis 黑洞（网络分区而非拒连）时收尾路径上的写会把 worker 挂到客户端的 `ReadTimeout`（可到分钟级）⇒ worker 池被拖垮、优雅停机卡住。修法：新增 `bestEffortRedis`——写放进 goroutine，最多等 5s，超时即放弃（best-effort 语义：失败只记日志）

## [v0.2.2] — 2026-10-28

- fix(redis): 空任务 ID 不被拒绝（父模块 v0.2.3 新增 `timertask.ErrEmptyTaskID`）——本模块的 `Send` 同步返回该错误；父模块依赖地板 v0.2.1 → v0.2.3

## [v0.2.1] — 2026-10-20

- build: 父模块依赖地板 v0.2.0 → v0.2.1（跟随 timertask v0.2.1）

## [v0.2.0] — 2026-10-19

### Breaking
- **从根包迁出**：`RedisBackend`/`NewRedisBackend`/`ShardManager`/`NewShardManager` 原位于
  `github.com/oekit/timertask` 根包，v0.2.0 起迁至本模块——import 路径替换即可，API 不变：

  ```go
  // 旧
  backend := timertask.NewRedisBackend(timertask.RedisConfig{...})
  // 新
  backend := redis.New(redis.RedisConfig{...})
  ```

  根包保留 `Task`/`Handler`/全部 `With*` 选项与 `LocalBackend`，既有本地形态调用点一行不动。

### build
- 本模块保留 `logx`/`concurrency`/`go-redis` 依赖（Redis 形态不在「纯 stdlib 嵌入式」约束内）
