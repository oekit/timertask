# Changelog — oekit/timertask/redis

由 oekit/timertask v0.2.0 拆分而来（Redis 形态独立成模块）。

## [v0.2.21] — 2026-11-13

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）（timertask#197）

关联问题单：timertask#197。

## [v0.2.20] — 2026-11-12

- 跟随 timertask v0.5.0 发版（release_train 嵌套 train）（timertask#167、timertask#168、timertask#169、timertask#170、timertask#171、timertask#172、timertask#173、timertask#174、timertask#175、timertask#176）

关联问题单：timertask#167、timertask#168、timertask#169、timertask#170、timertask#171、timertask#172、timertask#173、timertask#174、timertask#175、timertask#176。

## [v0.2.19] — 2026-11-11

- fix(redis): 租约回投收敛为 Lua 原子脚本 requeueLeaseScript——「先回投成功再摘租约」由服务端保证，回投写命令级失败（OOM / maxmemory 逐出 / WRONGTYPE）不再把任务从分片 ZSet 与 processing 两边一起摘掉、不进死信（#157）。go-redis Pipeline() 非 MULTI/EXEC、逐条独立执行，#150 的命令顺序反转（先 ZAdd 后 ZRem）堵不住「ZAdd 失败 + ZRem 成功」的半失败腿；脚本内 redis.call 出错即中止（后续命令不执行）：ZAdd 失败 ⇒ ZRem 不执行、租约保留、下轮回扫再投；ZAdd 成功后 ZRem 失败 ⇒ 任务暂留两处、下轮回扫重复回投——两腿都退化为「重复」而非「丢失」，at-least-once 允许重复、不容忍丢失。reapExpiredLeases 与 requeueClaim 两条回投路径统一走该脚本（issue 关切的「同一操作两种顺序」收敛为同一原子面）。

关联问题单：oekit/timertask#157。

- refactor(redis): 现代语言特性贴合处（#157 随行）——min/max 内建收敛 4 处手写钳制（leaseDeadline 短租补偿、续租周期/续租限时、回扫周期），for range int 收敛 2 处计数循环（Start 的 worker 池拉起、PendingCount 的分片扫描）；errors.Join 本模块无新增多错误聚合点（逐任务失败按 kv 日志逐条上报，聚合反而劣化可观测面）。

## [v0.2.18] — 2026-11-11

- fix(redis): reapExpiredLeases 重投反转为「先回投、后摘租约」——重投窗口内瞬时错误退化为重复投递而非任务永久丢失（#150）

关联问题单：oekit/timertask#150。

- fix(redis): 优雅停机的 claim 交接点 + 中断收尾——worker 抢走的 localQueue 任务要么执行、要么回灌，不再静默丢弃或进假死信（#151）

关联问题单：oekit/timertask#151。

- fix(redis): WithClock 对 RedisBackend 生效——BackendConfig 下发 Clock，redis 侧时间判断收敛到注入时钟（#152）

关联问题单：oekit/timertask#152。

- build: 依赖地板对齐——github.com/oekit/timertask v0.3.3（随本 train 发布）。

## [v0.2.17] — 2026-11-10

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.2.16] — 2026-11-08

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.2.15] — 2026-11-07

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.2.14] — 2026-11-07

- 跟随 timertask v0.2.12 发版（release_train 嵌套 train）（timertask#92、timertask#93、timertask#94、timertask#95、timertask#96、timertask#97、timertask#98、timertask#99）

关联问题单：timertask#92、timertask#93、timertask#94、timertask#95、timertask#96、timertask#97、timertask#98、timertask#99。

## [v0.2.13] — 2026-11-06

- 跟随 timertask v0.2.11 发版（release_train 嵌套 train）

## [v0.2.12] — 2026-11-06

- 跟随 timertask v0.2.11 发版（release_train 嵌套 train）

## [v0.2.11] — 2026-11-06

- 跟随 timertask v0.2.9 发版（release_train 嵌套 train）

## [v0.2.10] — 2026-11-06

- 跟随 timertask v0.2.9 发版（release_train 嵌套 train）

## [redis/v0.2.9] — 2026-11-06

- fix(redis): ZSet member 规范化——同拍重发不双跑（[#75](https://github.com/oekit/timertask/issues/75) ②）——member 此前是 task 原样序列化，而 Send 缺省给 CreateTime 填 now：同一任务同 fire_ts 拍两次 Send ⇒ CreateTime 不同 ⇒ member 不同 ⇒ ZSet 两条 ⇒ **双跑**。下游被迫在调用点钉死 `CreateTime = EndTime` 造字节一致换幂等，且这个坑只有读实现才知道（接口面零提示）。修法：member 改为规范化 JSON（拷贝后 CreateTime=EndTime，其余字段逐字节同输入），「同拍重发 = 同 member」成为库内不变量（ZAdd 同 member 更新 score 而非新增条目）。**行为副作用（升级可见）**：消费侧拿到的 `task.CreateTime == task.EndTime`——「创建时刻」在 ZSet member 语义里不再存在，需要真创建时刻的调用方自存业务字段；已钉 CreateTime=EndTime 的调用方（如 bianque）数值不变，钉死纪律可以退役。历史格式 member（CreateTime≠EndTime）解析不受影响，照常消费
- feat(redis): 新增 `Remove(ctx, taskID, endTS) (int64, error)` 撤约 API（[#75](https://github.com/oekit/timertask/issues/75) ①）——全包此前无 Remove/Cancel：业务侧停用/删除规则时无法即时撤约，只能到点触发后靠回调自校验收敛，窗口期内任务照跑（到点必空转一次）。实现按 (taskID → 分片 key，fireTS → score 精确窗口) 扫描匹配后 ZRem，幂等可重入（0 = 无匹配），未知任务无错返回 0。语义边界：只覆盖尚未被消费扫描 claim 的条目，与消费竞态时先到者得（Remove 赢=不触发；claim 赢=正常触发返回 0）；已 claim / 重试中 / 已进死信的任务不受影响

关联问题单：timertask#75。

## [redis/v0.2.8] — 2026-11-05

- 跟随 timertask v0.2.8 发版（release_train 嵌套 train）；内容 = 本文件 v0.2.8 条目的迁移示例修正

## [v0.2.8] — 2026-11-05

- docs: v0.2.0 迁移示例修正（[#73](https://github.com/oekit/timertask/issues/73)）——初版示例的两行 API 都是编造的（`timertask.RedisConfig` 结构体从未存在、本包没有 `func New(`），照抄必编译失败；真实签名两侧同形：`NewRedisBackend(buzType string, client goredis.UniversalClient, handler timertask.Handler, opts ...timertask.TimerTaskOption)`。同时把「import 路径替换即可」统一成「import 路径 + go.mod require 两处都要改」（本模块是独立 module，漏 require 报 `no required module provides package`）。历史条目就地修正（tag 内文本不可改），本条目供升级可见

## [v0.2.7] — 2026-11-03

- 跟随 timertask v0.2.7 发版（release_train 嵌套 train）（timertask#68）

关联问题单：timertask#68。

## [v0.2.6] — 2026-11-02

- 跟随 timertask v0.2.6 发版（release_train 嵌套 train）

## [v0.2.5] — 2026-10-29

- 跟随 timertask v0.2.5 发版（release_train 嵌套 train）

## [v0.2.4] — 2026-10-28

- 跟随 timertask v0.2.4 发版（release_train 嵌套 train）
- fix(redis): `taskKey` 复用已加固的 `shardIndex`（#42）——32 位平台不再把任务写进负分片的 key

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
  `github.com/oekit/timertask` 根包，v0.2.0 起迁至本模块——**import 路径与 go.mod require
  两处都要改**（本模块是独立 module，须 `go get github.com/oekit/timertask/redis@v0.2.0`
  新增 require 行；#73 修正：初版此处的「import 路径替换即可」与示例 API 均有误）：

  ```go
  // 旧（timertask v0.1.10，根包）
  backend := timertask.NewRedisBackend("buzType", goredisClient, handler, opts...)
  // 新（timertask/redis 模块；import 路径 + go.mod require 两处都要改）
  backend := redis.NewRedisBackend("buzType", goredisClient, handler, opts...)
  ```

  根包保留 `Task`/`Handler`/全部 `With*` 选项与 `LocalBackend`，既有本地形态调用点一行不动。

### build
- 本模块保留 `logx`/`concurrency`/`go-redis` 依赖（Redis 形态不在「纯 stdlib 嵌入式」约束内）
