# Changelog — oekit/timertask/redis

由 oekit/timertask v0.2.0 拆分而来（Redis 形态独立成模块）。

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
