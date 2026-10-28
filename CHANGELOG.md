# Changelog — oekit/timertask

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [redis/v0.2.4] — 2026-10-28

- fix(redis): taskKey 复用已加固的 shardIndex——32 位平台不再产出负分片

`taskKey` 用 `int(crc32.ChecksumIEEE(...)) % b.buckets` 选分片，**没有复用同仓已经加固过的 `shardIndex`** ⇒ 在 **32 位平台**（`GOARCH=386` / `arm`）上 `int(uint32)` 按位截断成负数 ⇒ 分片下标为负 ⇒ 任务被写进 `{timertask.<type>}.shard.-3` 这样的 key，而消费/回扫只扫 `shard.0 … shard.N-1` ⇒ **任务永远不会被执行，也不会进死信**，且不出现在 `PendingCount`（只数合法分片）⇒ 静默丢任务，排障完全无痕迹；at-least-once 模式的租约回扫也写回负分片，同样救不回来。

更值得记的是**护栏失守**：仓里的源码级护栏 `TestShardIndex_NoIntTruncationInSource` **只读 `shard.go`**（对同包的 `redis.go` 完全失明），且匹配的是**字面量** `int(hash)`（真实写法 `int(crc32.ChecksumIEEE([]byte(taskID)))` 匹配不到）⇒ 即使把扫描范围扩到全目录，字面量也匹配不上 ⇒ 「同型缺陷在别的文件里再出现一次」没有任何机制会报。

三处修：① `taskKey` 改走 `shardIndex`（先在 uint32 空间取模再转 int，同仓已有且带注释的正确实现）；② 源码护栏升级为「扫全目录 + **形态匹配**」——找「把哈希值强转成 int」且「取模在转换**之外**」的行（危险形态 `int(crc32...(...)) % n` 命中，安全形态 `int(hash % uint32(n))` 不误伤），且用 `ast` 判定并跳过注释/文档串；③ 补两条行为护栏：5000 个任务 ID 的分片段恒在 `[0, buckets)`，以及旧写法在 32 位平台上确实给出负分片（钉住缺陷本身，避免护栏退化成空转检查）。

另修掉嵌套模块 `redis/retry_semantics_test.go` 两处既有 govet shadow——嵌套模块从不被 CI lint（共享 workflow 的 lint job 只跑仓根，嵌套模块只 build+test）⇒ 这类问题一直隐形（#42）

## [v0.2.4] — 2026-10-28

- fix(redis): taskKey 复用已加固的 shardIndex——32 位平台不再产出负分片

`taskKey` 用 `int(crc32.ChecksumIEEE(...)) % b.buckets` 选分片，**没有复用同仓已经加固过的 `shardIndex`** ⇒ 在 **32 位平台**（`GOARCH=386` / `arm`）上 `int(uint32)` 按位截断成负数 ⇒ 分片下标为负 ⇒ 任务被写进 `{timertask.<type>}.shard.-3` 这样的 key，而消费/回扫只扫 `shard.0 … shard.N-1` ⇒ **任务永远不会被执行，也不会进死信**，且不出现在 `PendingCount`（只数合法分片）⇒ 静默丢任务，排障完全无痕迹；at-least-once 模式的租约回扫也写回负分片，同样救不回来。

更值得记的是**护栏失守**：仓里的源码级护栏 `TestShardIndex_NoIntTruncationInSource` **只读 `shard.go`**（对同包的 `redis.go` 完全失明），且匹配的是**字面量** `int(hash)`（真实写法 `int(crc32.ChecksumIEEE([]byte(taskID)))` 匹配不到）⇒ 即使把扫描范围扩到全目录，字面量也匹配不上 ⇒ 「同型缺陷在别的文件里再出现一次」没有任何机制会报。

三处修：① `taskKey` 改走 `shardIndex`（先在 uint32 空间取模再转 int，同仓已有且带注释的正确实现）；② 源码护栏升级为「扫全目录 + **形态匹配**」——找「把哈希值强转成 int」且「取模在转换**之外**」的行（危险形态 `int(crc32...(...)) % n` 命中，安全形态 `int(hash % uint32(n))` 不误伤），且用 `ast` 判定并跳过注释/文档串；③ 补两条行为护栏：5000 个任务 ID 的分片段恒在 `[0, buckets)`，以及旧写法在 32 位平台上确实给出负分片（钉住缺陷本身，避免护栏退化成空转检查）。

另修掉嵌套模块 `redis/retry_semantics_test.go` 两处既有 govet shadow——嵌套模块从不被 CI lint（共享 workflow 的 lint job 只跑仓根，嵌套模块只 build+test）⇒ 这类问题一直隐形（#42）

## [redis/v0.2.3] — 2026-10-28

- fix(redis): 分片下标 `int(hash) % len(clients)` 在 **32 位平台**上 uint32→int 截断成负数 ⇒ 负下标越界 panic，分片分配 goroutine 当场死亡（心跳门控还关着 ⇒ 实例静默不消费任务）。修法：先在 uint32 空间取模再转 int
- fix(redis): 死信写入与终态 Ack 此前用 `context.Background()` 调 Redis ⇒ Redis 黑洞（网络分区而非拒连）时收尾路径上的写会把 worker 挂到客户端的 `ReadTimeout`（可到分钟级）⇒ worker 池被拖垮、优雅停机卡住。修法：新增 `bestEffortRedis`——写放进 goroutine，最多等 5s，超时即放弃（best-effort 语义：失败只记日志）

## [redis/v0.2.2] — 2026-10-28

- fix(redis): 空任务 ID 不被拒绝（父模块 v0.2.3 新增 `timertask.ErrEmptyTaskID`）——本模块的 `Send` 同步返回该错误；父模块依赖地板 v0.2.1 → v0.2.3

## [v0.2.3] — 2026-10-28

- fix(timertask): 空任务 ID 不被拒绝——ID 注释写着「任务唯一 ID」，但两个后端都不校验 ⇒ 空 ID 的任务在 local 后端只有第一个入队、其余**静默丢弃**，在 redis 后端则全部入队、各自执行一次。构造期直接报错（新增 `ErrEmptyTaskID`），把「调用方漏填 ID」挡在提交那一刻
- fix(timertask): local 后端同 ID 重复提交时既不报错也不记日志，调用方无法判断本次是否真的入队；补一条日志让「这次没入队」在日志里可见，并把「先到先得、不入队、返回 nil（幂等提交成功）」的口径写进文档

## [v0.2.2] — 2026-10-28

- fix(redis): 重试窗口内被中断仍 Ack 租约 ⇒ 任务**永久丢失**——defer 里的 Ack 在每条返回路径上都执行（含 ctx 取消 / 停机那条，与同处注释「仅 handler 仍在重试窗口内不 Ack」直接矛盾），任务已从分片 ZSet claim 走、租约是它唯一的去处，释放了就落到「哪都不是」的地方 ⇒ at-least-once 承诺失效
- fix(timertask): 租约回扫的「重试耗尽」分支**不可达** ⇒ `MaxRetry<=0` 的任务被无限重投（既不进死信、也不停止）——判定带了 `&& task.MaxRetry > 0` 守卫，且回扫侧未做 worker 侧那套负数归一 ⇒ 同一份任务两条路径语义分叉；另把 `MaxRetry` 的三态语义（0=用后端默认 / >0=显式次数 / <0=不重试）写进 godoc
- fix(local): 调度器用**裸 `go`** 启动内部 goroutine，违反「禁止裸 go func」硬规则——用户任务的 panic 已由 `invoke` 就地收编，但调度器自身（cron 计算、触发点记账）的 panic 仍会打死整个进程；`local` 包自带最小 `goSafe`（与根包同款形态；本包刻意不 import 根包，依赖面保持不变）

## [redis/v0.2.1] — 2026-10-20

- build: 父模块依赖地板 v0.2.0 → v0.2.1（跟随 timertask v0.2.1；父模块地板不得落后最新 tag）

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