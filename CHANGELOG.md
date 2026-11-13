## [v0.5.4] — 2026-11-13

- 租约/死信原子面：回投 member 字节稳定 + retry_count 迁 hash（#188）、RunWithRetry 基线取入参 RetryCount（#189）、日志 err 键根模块单源常量（#193）、死信写入与租约 Ack 原子化防毒丸无痕丢失（#197）、回归假绿修复 + RetryExhausted at-most-once 分支（#214）

关联问题单：timertask#188、timertask#189、timertask#193、timertask#197、timertask#214。

## [redis/v0.2.21] — 2026-11-13

- fix: 死信+摘租约 Lua 原子面——四路径半失败不再丢任务（timertask#197）。deadLetterAckScript 先死信失败即中止（租约保留重试），死信/租约两 member 分离保 #188/#189 计数口径；RetryExhausted 失败不置 terminal。

关联问题单：timertask#197。

## [v0.5.3] — 2026-11-13

- fix(redis): 死信+摘租约 Lua 原子面——四路径半失败不再丢任务（#197）。新增 deadLetterAckScript：写失败即中止（租约保留下轮重试），成功后裁剪+摘租约；死信 member=marshal(task) 真实计数（#189）、摘租约 member=原始字节（#188）两参数分离。RetryExhausted 失败不置 terminal、onFailure 移到确认进死信后。

关联问题单：timertask#197。

## [v0.5.2] — 2026-11-13

- 3 单修复（L13 车道 #188/#189/#193）；地板对齐 pkg v0.47.0。

## [v0.5.1] — 2026-11-12

- Go 1.26 现代化：redis/shard.go sort.Strings→slices.Sort、local/scheduler.go 计数循环→range N；嵌套 redis 地板对齐 concurrency v0.34.1。纯等价重构+地板追平，行为零变更。

## [redis/v0.2.20] — 2026-11-12

- refactor(local,cron): 全仓重构收敛——Scheduler「entries+keyed 双表快照」四处收集块收敛为 snapshotEntries()（#167）；goSafe 同模块三份拷贝（logging.go/local/cron）收敛到 internal/gosafe 单源、日志统一补 stack 字段、修正 local「刻意不 import 根包」失实注释（#168）；cron spec「解析+时区+可满足性」三连抄收敛为 parseSpec/checkSatisfiable（三处错误文案逐字不变，#170）；mustJSON 测试辅助从生产文件挪进 _test.go（#172）
- refactor: local 与 redis 两后端 processTask 重试主循环收敛为根包导出的 RunWithRetry（lazy 重试 timer + 退避 select + panic 收编 + RetryCount 推进单点实现），后端差异留 RetryHooks 钩子（local 中断丢弃 / redis handleInterruptedClaim）；NormalizeRetry、ErrBackendAlreadyStarted 一并导出，redis 嵌套模块删除被迫自持的 helpers 复制（#169）
- fix: 「recover→error」收编助手四份形态（runHandlerSafe / local.Scheduler.invoke / cron.Scheduler.execSafe / cron.Scheduler.record）统一走 pkg errors.Recovered 单源（#171）；runHandlerSafe panic 收编从 fmt.Errorf(%v) 改走 errors.Recovered——panic 值是 error 时身份透传（errors.Is/As 可判定原始错误，此前重试判定/死信归因断链）、goroutine 栈恒在场（#173，附回归测试）
- fix(local,cron): 对账循环 clock.After 每轮新建不可停 timer、被 wake/停机打断时遗弃堆积（堆积量=打断频率×剩余等待）→ local.Clock 接口新增 NewTimer 可停 timer 形态（SystemClock/FakeClock 同步实现），scheduler.run 与 cron.StartReconcile 醒来/退出即 Stop（#174，附回归测试）
- fix(redis): 毒丸 member 静默丢弃 → 原样写入死信 ZSet 取证（claim 与租约回扫两路径，score 取当前时刻），at-least-once 下补摘租约止损（随嵌套 redis/v0.2.20 发布，#175，附 miniredis 毒丸专项测试×3）
- fix: WithRetry 构造期校验——负 count/负 delay 直接 panic（对齐同簇「配置错误启动即暴露」纪律；此前负 count 被下游 normalizeRetry 隐式归 0 静默变义、负 delay 变忙重试；0 值合法）（#176，附回归测试）
- chore: Go 地板 1.24.0 → 1.26.0（根与 ci.yml/lint 配置；嵌套 redis 已就位）；pkg 依赖 v0.44.0 → v0.45.0（panic 收编单源 errors.Recovered）。**BREAKING**：local.Clock 接口新增 NewTimer(d time.Duration) Timer 方法——外部自定义 Clock 实现需补该方法（0.x minor 允许 breaking）

关联问题单：timertask#167、timertask#168、timertask#169、timertask#170、timertask#171、timertask#172、timertask#173、timertask#174、timertask#175、timertask#176。

## [v0.5.0] — 2026-11-12

- refactor(local,cron): 全仓重构收敛——Scheduler「entries+keyed 双表快照」四处收集块收敛为 snapshotEntries()（#167）；goSafe 同模块三份拷贝（logging.go/local/cron）收敛到 internal/gosafe 单源、日志统一补 stack 字段、修正 local「刻意不 import 根包」失实注释（#168）；cron spec「解析+时区+可满足性」三连抄收敛为 parseSpec/checkSatisfiable（三处错误文案逐字不变，#170）；mustJSON 测试辅助从生产文件挪进 _test.go（#172）
- refactor: local 与 redis 两后端 processTask 重试主循环收敛为根包导出的 RunWithRetry（lazy 重试 timer + 退避 select + panic 收编 + RetryCount 推进单点实现），后端差异留 RetryHooks 钩子（local 中断丢弃 / redis handleInterruptedClaim）；NormalizeRetry、ErrBackendAlreadyStarted 一并导出，redis 嵌套模块删除被迫自持的 helpers 复制（#169）
- fix: 「recover→error」收编助手四份形态（runHandlerSafe / local.Scheduler.invoke / cron.Scheduler.execSafe / cron.Scheduler.record）统一走 pkg errors.Recovered 单源（#171）；runHandlerSafe panic 收编从 fmt.Errorf(%v) 改走 errors.Recovered——panic 值是 error 时身份透传（errors.Is/As 可判定原始错误，此前重试判定/死信归因断链）、goroutine 栈恒在场（#173，附回归测试）
- fix(local,cron): 对账循环 clock.After 每轮新建不可停 timer、被 wake/停机打断时遗弃堆积（堆积量=打断频率×剩余等待）→ local.Clock 接口新增 NewTimer 可停 timer 形态（SystemClock/FakeClock 同步实现），scheduler.run 与 cron.StartReconcile 醒来/退出即 Stop（#174，附回归测试）
- fix(redis): 毒丸 member 静默丢弃 → 原样写入死信 ZSet 取证（claim 与租约回扫两路径，score 取当前时刻），at-least-once 下补摘租约止损（随嵌套 redis/v0.2.20 发布，#175，附 miniredis 毒丸专项测试×3）
- fix: WithRetry 构造期校验——负 count/负 delay 直接 panic（对齐同簇「配置错误启动即暴露」纪律；此前负 count 被下游 normalizeRetry 隐式归 0 静默变义、负 delay 变忙重试；0 值合法）（#176，附回归测试）
- chore: Go 地板 1.24.0 → 1.26.0（根与 ci.yml/lint 配置；嵌套 redis 已就位）；pkg 依赖 v0.44.0 → v0.45.0（panic 收编单源 errors.Recovered）。**BREAKING**：local.Clock 接口新增 NewTimer(d time.Duration) Timer 方法——外部自定义 Clock 实现需补该方法（0.x minor 允许 breaking）

关联问题单：timertask#167、timertask#168、timertask#169、timertask#170、timertask#171、timertask#172、timertask#173、timertask#174、timertask#175、timertask#176。

## [v0.4.0] — 2026-11-11

- feat(cron): 新增 `timertask/cron` 动态 cron 装配子包（#159）——「多实例动态 cron 装配 + 执行互斥 + 运行台账」三缺口单点收编（下游 3 处各自手搓调度器的公共面）：`Plan{ID,Spec,Fingerprint,Enabled}` 期望集 + `Reconcile` 指纹 diff 增量重装（指纹未变幂等空操作、不重置 @every 触发基准——防周期重装把长间隔任务永不触发；指纹变化摘旧装新）+ `Upsert/Remove` 运行期单条增改删 + `StartReconcile` 启动清扫（`WithStaleSweep`，zeus/task FailStaleRunning「启动清扫+补发事件」同序）与周期对账循环（listFn 失败/单条坏 spec 只记日志不中断，errors.Join 逐条汇总）。触发引擎复用同模块 local.Scheduler 既有基建（表达式解析/catch-up/并发度/假时钟），根模块 require 仍仅 pkg（go.mod 零变更）。
- feat(cron): 执行互斥进库（#159）——`Mutex` 最小契约（TryLock/Unlock 与 redisx/distlock.Locker 同形，**可选依赖 distlock 的缝**：仓内不引 redisx，distlock.Locker 包一层哨兵映射即接入，godoc 附 6 行适配示例）+ `LocalMutex` 进程内租约实现（内建 slot 守护 + 跨实例测试共享假件）。互斥语义 = **slot 认领**：触发点租约不随执行完成释放（兼作「已认领」标记），迟到重复触发在租约有效期内一律让位——同一触发点跨实例恰好一次；崩溃实例租约到期自动接管（与仓内 at-least-once 租约同型）；互斥设施故障 fail-closed 跳过（宁可漏跑一轮可补发，不可双跑）。**锁 TTL 预算公式单一出处** `RetryBudget.LockTTL() = Attempts*Timeout + TotalBackoff() + Slack`（与 zeus/task execLockTTL 同口径收编——此前散在下游手算，口径漂移即互斥提前过期、同 slot 双跑）。
- feat(cron): 运行台账（#159）——`RunRecord{PlanID,Trigger,Instance,StartedAt,EndedAt,Err,Skipped}`（成功/失败/让位/设施故障四态可辨、Instance 多实例归因）+ `Ledger` 外置落库缝（与内置有界 `MemLedger` 并联，RunHistory 读取面），台账 panic 收编不影响触发。Executor 的 trigger 串（RFC3339 秒精度）与互斥键/台账键三同源，业务可直接作幂等键。
- 现代语言特性：sync.OnceFunc 收编触发引擎拉起与停机收口两处 once 语义（startEngineOnce/stopOnce，天然幂等）；errors.Join 聚合 Reconcile 逐条失败（与 local.Reconcile 同款，多错误一次给出）；min/max 内建收编 RetryBudget 归一钳制（Attempts/Timeout/Backoff）；for range int 收敛 MemLedger 环形回卷拷贝与并发争抢测试的计数循环。

关联问题单：oekit/timertask#159。

## [v0.3.4] — 2026-11-11

- fix(local): ScheduleFor 对幂等空操作的 nil Task 不再报错（#155）——`fn == nil` 检查从入口前置移入「需要安装/重装」分支，与 Reconcile「spec 未变时 Task 被忽略」同口径：同 key 同 spec 的 `ScheduleFor(key, spec, nil)` 幂等空操作放行（纯表达式 upsert，任务函数以首次注册为准），新 key / spec 变化仍要求 Task 非 nil（与 Reconcile 安装分支同序）。修复前入口在幂等判定之前就报 "nil task"，两个入口口径分裂，调度平台型消费方被迫 UnscheduleFor 再注册（摘旧装新重置触发基准——@every 后移陷阱）或塞哑函数（同样重置基准）。现代语言特性：本次改动面 errors.Join 无新增聚合点（Reconcile 逐条汇总已是 errors.Join 形态）。本 train 同时发布 redis/v0.2.19（#157 的完整修复叙事在该版本节）。

关联问题单：oekit/timertask#155。

## [redis/v0.2.19] — 2026-11-11

- fix(redis): 租约回投收敛为 Lua 原子脚本 requeueLeaseScript——「先回投成功再摘租约」由服务端保证，回投写命令级失败（OOM / maxmemory 逐出 / WRONGTYPE）不再把任务从分片 ZSet 与 processing 两边一起摘掉、不进死信（#157）。go-redis Pipeline() 非 MULTI/EXEC、逐条独立执行，#150 的命令顺序反转（先 ZAdd 后 ZRem）堵不住「ZAdd 失败 + ZRem 成功」的半失败腿；脚本内 redis.call 出错即中止（后续命令不执行）：ZAdd 失败 ⇒ ZRem 不执行、租约保留、下轮回扫再投；ZAdd 成功后 ZRem 失败 ⇒ 任务暂留两处、下轮回扫重复回投——两腿都退化为「重复」而非「丢失」，at-least-once 允许重复、不容忍丢失。reapExpiredLeases 与 requeueClaim 两条回投路径统一走该脚本（issue 关切的「同一操作两种顺序」收敛为同一原子面）。

关联问题单：oekit/timertask#157。

- refactor(redis): 现代语言特性贴合处（#157 随行）——min/max 内建收敛 4 处手写钳制（leaseDeadline 短租补偿、续租周期/续租限时、回扫周期），for range int 收敛 2 处计数循环（Start 的 worker 池拉起、PendingCount 的分片扫描）；errors.Join 本模块无新增多错误聚合点（逐任务失败按 kv 日志逐条上报，聚合反而劣化可观测面）。

## [v0.3.3] — 2026-11-11

- fix: BackendConfig 增加 Clock 下发通道（#152）——WithClock 对 RedisBackend 静默 no-op 的根因是 ResolveOptions 不下发 Clock、redis 侧时间判断硬编码 time.Now；API 增量（可选字段，向后兼容），配合 redis/v0.2.18 使 FakeClock 注入对 Redis 侧生效。本 train 同时发布 redis/v0.2.18（#150、#151、#152 的完整修复叙事在该版本节）。

关联问题单：oekit/timertask#150、oekit/timertask#151、oekit/timertask#152。

## [redis/v0.2.18] — 2026-11-11

- fix(redis): reapExpiredLeases 重投反转为「先回投、后摘租约」——重投窗口内瞬时错误退化为重复投递而非任务永久丢失（#150）

关联问题单：oekit/timertask#150。

- fix(redis): 优雅停机的 claim 交接点 + 中断收尾——worker 抢走的 localQueue 任务要么执行、要么回灌，不再静默丢弃或进假死信（#151）

关联问题单：oekit/timertask#151。

- fix(redis): WithClock 对 RedisBackend 生效——BackendConfig 下发 Clock，redis 侧时间判断收敛到注入时钟（#152）

关联问题单：oekit/timertask#152。

- build: 依赖地板对齐——github.com/oekit/timertask v0.3.3（随本 train 发布）。

## [v0.3.2] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.3.1] — 2026-11-10

- - build: 依赖地板对齐（第二轮发版后 floor sync，无代码变更）

## [redis/v0.2.17] — 2026-11-10

- - build: 嵌套地板发布——timertask v0.3.0（floor sync，无代码变更）

## [v0.3.0] — 2026-11-10

- - feat(local): 按业务键的动态条目管理——ScheduleFor（同 key 同 spec 幂等）/UnscheduleFor/Reconcile 期望集收敛，单条坏 spec 不阻断收敛（#129）

关联问题单：timertask#129。

## [v0.2.14] — 2026-11-09

- fix(local): Schedule 拒绝永无触发点的 cron spec（#123）
- fix(local/redis): 二次 Start 与停机后 Send 显式报错 ErrBackendStopped（#124）
- fix(redis): 停机回灌/续租弃子限时——退出路径不再卡 ReadTimeout（#121 #122）

关联问题单：timertask#121、timertask#122、timertask#123、timertask#124。

## [redis/v0.2.16] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.2.13] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [redis/v0.2.15] — 2026-11-07

- build: 父模块地板对齐（嵌套 go.mod 的父仓 require 跟随最新，audit 门禁项）

## [redis/v0.2.14] — 2026-11-07

- fix(local): scanOnce 锁内只弹出到期任务、发送移到锁外——满队列+慢 handler 不再整体卡死 Send/PendingCount（#92 #94）
- fix(redis): at-least-once 租约按 visibilityTimeout/3 周期续期——慢 handler 不再被回扫必现重复投递（#93 #95）
- fix(redis): 停机窗口 claim-入队间隙的任务回灌 Redis——at-most-once 静默丢弃兜底失效关闭（#96）
- fix(send): 两个后端 Send 均入队前深拷贝 Task——不再原地改写调用方 MaxRetry/CreateTime（#97）
- fix(local): 同 ID 去重覆盖排队态与执行窗口——inflight 集合占用，处理结束后可重新提交（#98）
- fix(scheduler): Stop 触发的正常停机 Start 返回 nil、ctx 取消返回 ctx.Err()——优雅停机不再被伪装成失败（#99）

关联问题单：timertask#92、timertask#93、timertask#94、timertask#95、timertask#96、timertask#97、timertask#98、timertask#99。

## [v0.2.12] — 2026-11-07

- fix(local): scanOnce 锁内只弹出到期任务、发送移到锁外——满队列+慢 handler 不再整体卡死 Send/PendingCount（#92 #94）
- fix(redis): at-least-once 租约按 visibilityTimeout/3 周期续期——慢 handler 不再被回扫必现重复投递（#93 #95）
- fix(redis): 停机窗口 claim-入队间隙的任务回灌 Redis——at-most-once 静默丢弃兜底失效关闭（#96）
- fix(send): 两个后端 Send 均入队前深拷贝 Task——不再原地改写调用方 MaxRetry/CreateTime（#97）
- fix(local): 同 ID 去重覆盖排队态与执行窗口——inflight 集合占用，处理结束后可重新提交（#98）
- fix(scheduler): Stop 触发的正常停机 Start 返回 nil、ctx 取消返回 ctx.Err()——优雅停机不再被伪装成失败（#99）

关联问题单：timertask#92、timertask#93、timertask#94、timertask#95、timertask#96、timertask#97、timertask#98、timertask#99。

## [redis/v0.2.13] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [redis/v0.2.12] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [v0.2.11] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [redis/v0.2.11] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [redis/v0.2.10] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.2.10] — 2026-11-06

- feat: `WithClock(Clock)` 时钟注入——`LocalBackend` 的时间判断（CreateTime 缺省、扫描触发点）改走注入时钟（[#78](https://github.com/oekit/timertask/issues/78)）：同模块 `local/scheduler` 早已提供 Clock 注入（FakeClock 确定性测试），而 LocalBackend 三处硬编码 `time.Now()`——同模块两个 local 形态对可测试时钟的态度相反且未声明，时钟敏感逻辑（租约/过期判定）实际不可测。修法：根包新增 `Clock` 接口（Now）+ `SystemClock`，`WithClock` 注入（nil 取 SystemClock）；local.Clock 改别名嵌入根定义（单一事实源，保留调度器需要的 After）。回归：FakeClock 拨到触发点之后扫描即触发，不真睡
- fix(local): `Scheduler.execute` 的 sem 阻塞可被 stopCh 唤醒（[#79](https://github.com/oekit/timertask/issues/79)）——此前 select 只监听 sem 与 Start 传入的 ctx：Start 用不可取消 ctx（生产常态）时，并发配额占满后到达的任务阻塞在 sem 上，`Stop` 触发 stopCh 却叫不醒它——关停面留尾巴：进程想快速退出，却有 runner 卡在配额闸上无期限等待。修法：select 并入 stopCh（停止信号优先，直接返回不执行该轮）

关联问题单：timertask#78、timertask#79。

## [v0.2.9] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.2.8] — 2026-11-05

- docs: v0.2.0/v0.2.1 迁移示例修正（[#73](https://github.com/oekit/timertask/issues/73)）——示例的两行 API 都是编造的（`timertask.RedisConfig` 结构体从未存在——v0.1.10 真实签名是 `NewRedisBackend(buzType string, client redis.UniversalClient, handler Handler, opts ...TimerTaskOption)`；redis 包没有 `func New(`——v0.2.x 真实签名是 `redis.NewRedisBackend(buzType string, client goredis.UniversalClient, handler timertask.Handler, opts ...timertask.TimerTaskOption)`），照抄必编译失败（bianque 升级实录：读示例不够、还得去 module cache 读真实签名才迁成功）。历史条目就地修正为真实签名；redis 子模块的「import 路径替换即可」同步统一成「import 路径 + go.mod require 两处都要改」。仅文档修正，无代码变更

## [redis/v0.2.8] — 2026-11-05

- 跟随 timertask v0.2.8 发版（release_train 嵌套 train）；内容 = redis 迁移示例修正（timertask#73）

## [redis/v0.2.7] — 2026-11-03

- build: 把父仓 timertask v0.2.7 与 concurrency v0.23.3、logx v0.8.2 的地板带进已发布产物（redis/v0.2.6 的快照嵌的是 concurrency v0.23.2、logx v0.8.0，消费方拿不到新内容）（timertask#68）

关联问题单：timertask#68。

## [v0.2.7] — 2026-11-03

- build: 把父仓 timertask v0.2.7 与 concurrency v0.23.3、logx v0.8.2 的地板带进已发布产物（redis/v0.2.6 的快照嵌的是 concurrency v0.23.2、logx v0.8.0，消费方拿不到新内容）（timertask#68）

关联问题单：timertask#68。

## [redis/v0.2.6] — 2026-11-02

- build: 矩阵内依赖地板 `pkg` v0.31.0 → v0.31.1（维护轮全量对齐；本版不含 API 变更）

## [v0.2.6] — 2026-11-02

- build: 矩阵内依赖地板 `pkg` v0.31.0 → v0.31.1（维护轮全量对齐；本版不含 API 变更）

## [redis/v0.2.5] — 2026-10-29

- build: 依赖地板对齐——concurrency 抬到 v0.23.2

## [v0.2.5] — 2026-10-29

- build: 依赖地板对齐——concurrency 抬到 v0.23.2

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
- 正确的迁移步骤（**新增 require 是必需的一步**）（#19）：

  ```bash
  go get github.com/oekit/timertask/redis@v0.2.0
  ```

  ```go
  // 旧（timertask v0.1.10，根包）
  backend := timertask.NewRedisBackend("buzType", goredisClient, handler, opts...)
  // 新（timertask/redis 模块；import 路径 + go.mod require 两处都要改）
  backend := redis.NewRedisBackend("buzType", goredisClient, handler, opts...)
  ```

- 仅文档修正，无代码变更

## [v0.2.0] — 2026-10-19

### Breaking
- **Redis 后端迁至嵌套模块**（#14）：`RedisBackend`/`NewRedisBackend`/`ShardManager`/`NewShardManager`
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