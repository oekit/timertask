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
//     由调用方幂等）。
//
// 兜底路径：
//
//   - 优雅停机（ctx 取消）时，本地队列中尚未处理的任务会回灌 Redis，
//     重启后继续消费；
//   - 重试耗尽的任务会写入持久死信 ZSet（{timertask.<buzType>}.dead，
//     score 为失败时间戳，默认保留最近 10000 条）；WithOnFailure 回调
//     只是死信之外的进程内通知手段，不构成持久化。
//
// 哪种任务选哪种模式：
//
//   - 提醒/审计/计费等不可丢事件 → DeliveryAtLeastOnce + 调用方幂等；
//   - 可丢弃的指标/通知/缓存预热 → DeliveryAtMostOnce（更省 Redis 往返）。
package redis
