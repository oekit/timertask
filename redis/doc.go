// Package redis 提供基于 Redis ZSet 的分布式延迟任务后端。
//
// 本包是独立 module（module path：github.com/oekit/timertask/redis），
// 与父模块 github.com/oekit/timertask 分开演进：
//
//   - 父模块（根包）只保留通用类型与本地内存后端 LocalBackend，
//     依赖面里没有 redis——纯 Go 嵌入式服务形态（CGO_ENABLED=0 静态链接、
//     SQLite 落盘、零外部中间件）可以直接用；
//   - 本模块承载 Redis 形态（分片管理、心跳、可靠投递），
//     显式引入才会把 go-redis 拉进依赖面。
//
// # 迁移路径（破坏性变更）
//
// Redis 后端原在根包 timertask 内，本次拆分后从根包迁出：
//
//	// 旧：import "github.com/oekit/timertask"
//	//     b := timertask.NewRedisBackend("order", client, handler)
//	// 新：import "github.com/oekit/timertask/redis"
//	//     b := redis.NewRedisBackend("order", client, handler)
//
// 其余符号对照：
//
//	根包 timertask.NewRedisBackend      → redis.NewRedisBackend
//	根包 timertask.RedisBackend         → redis.RedisBackend
//	根包 timertask.NewShardManager      → redis.NewShardManager
//	根包 timertask.ShardManager         → redis.ShardManager
//
// 选项函数仍留在根包（写法不变，只是包路径不同）：
//
//	redis.NewRedisBackend("order", client, handler, timertask.WithBuckets(64))
//
// 任务类型也仍在根包：
//
//	func handler(ctx context.Context, task *timertask.Task) error { ... }
//
// # 分片与 Redis Cluster（单 slot 逻辑分片，#292）
//
// WithBuckets(n) 把任务按 crc32(taskID) 散到 n 个逻辑分片，**但所有键共用
// 同一个 hash tag `{timertask.<buzType>}`**：
//
//	{timertask.<buzType>}.shard.N        分片 ZSet
//	{timertask.<buzType>}.processing     租约集
//	{timertask.<buzType>}.retry_count    累计重试计数 hash
//	{timertask.<buzType>}.dead           死信 ZSet
//	{timertask.<buzType>}.heartbeat      实例心跳/分片归属 ZSet
//
// 因此 Redis Cluster 下同一 buzType 的全部数据落在**同一个 slot**：分片只是
// 单节点内的逻辑分片（锁/批量扫描粒度、本地队列隔离），不提供跨节点吞吐、
// 内存与故障域扩展。单 slot 是当前设计的硬边界，选型时按单节点容量规划；
// 需要跨节点扩展请等待下述 breaking 变更。
//
// 把 hash tag 下沉到分片粒度（`{timertask.<buzType>.shard.N}`）需要把
// processing/retry_count/dead 一并改成按分片键（claim/回投/死信 Ack 的 Lua
// 脚本要求同 slot 原子性），这是**键空间 breaking 变更**：升级后旧键对新
// 版本不可见，必须配套迁移（升级前 flush 旧键，等价于丢弃存量任务）。该
// 变更登记在后续 breaking 窗口实施，不在本次修复中静默切换——避免一个
// 补丁升级把运维的存量任务整体变成孤儿键。
//
// # 消费语义
//
// 支持两种投递模式（timertask.WithDeliveryMode / timertask.WithAtLeastOnce）：
//
//   - DeliveryAtMostOnce（默认，与历史行为一致）：消费流程先通过 Lua 脚本
//     原子删除 ZSet 中的任务条目，再推入本地 worker 队列处理。进程在删除后、
//     处理前崩溃，任务永久丢失。应用层需自行保证幂等性。
//   - DeliveryAtLeastOnce（可靠模式）：claim 时把条目移入 processing 租约集
//     （score=租约到期），handler 成功后 Ack 删除；租约超时由回扫按 RetryCount
//     重投。kill -9 后未 Ack 任务会在 visibility timeout 后被重投（可容忍重复，
//     由调用方幂等）。handler 执行期间由后台周期续租（每 visibilityTimeout/3
//     推一次 deadline）：慢于 visibilityTimeout 但最终成功的 handler 不会被
//     回扫误判死亡而必现重复投递——「租约到期 = 持有者已死」仅在持有者停止
//     续租（崩溃/退出）后才成立。
//
// 兜底路径：
//
//   - 优雅停机（ctx 取消）时，本地队列中尚未处理的任务会回灌 Redis，
//     重启后继续消费；claim 与入队之间停机窗口内的在途任务同样回灌
//     （不会静默丢弃）；
//   - 重试耗尽的任务会写入持久死信 ZSet（{timertask.<buzType>}.dead，
//     score 为失败时间戳，默认保留最近 10000 条）；WithOnFailure 回调
//     只是死信之外的进程内通知手段，不构成持久化。
//
// # 提交与撤销
//
//   - Send 的 ZSet member 是任务的规范化 JSON（CreateTime 钉为 EndTime）：
//     同一任务同一 fire_ts 拍两次 Send = 同 member = ZSet 单条（ZAdd 更新
//     score 而非新增），「同拍重发不双跑」是库内不变量。副作用：消费侧
//     task.CreateTime == task.EndTime，需要真创建时刻的调用方自存业务字段。
//   - Remove(taskID, fireTS) 撤销尚未触发的条目（幂等，返回撤销数）；已
//     claim / 重试中 / 已进死信的任务不受影响——撤销要赶在 fireTS 之前。
//
// 哪种任务选哪种模式：
//
//   - 提醒/审计/计费等不可丢事件 → DeliveryAtLeastOnce + 调用方幂等；
//   - 可丢弃的指标/通知/缓存预热 → DeliveryAtMostOnce（更省 Redis 往返）。
package redis
