// Package timertask 提供延迟任务队列的通用类型与本地（纯内存）后端。
//
// # 模块边界
//
// 本包（module github.com/oekit/timertask）是**零 Redis 依赖**的：
//
//   - 本包 LocalBackend：基于内存的本地延迟任务（单实例场景），
//     依赖面只有标准库 + github.com/oekit/pkg（仅 local 子包的 cron 解析用）；
//   - github.com/oekit/timertask/local：本地 cron 调度器（子包，同模块），
//     按 cron 表达式触发任务，同样零 Redis；
//   - github.com/oekit/timertask/redis：基于 Redis ZSet 的分布式延迟任务
//     （**独立 module**，需要显式引入才会拉进 go-redis）。
//
// 拆分动因：Go 按包编译，此前 Redis 后端与本地后端同处一个 package，
// 导致「import timertask」必然拉进 go-redis——纯 Go 嵌入式服务形态
// （CGO_ENABLED=0 静态链接、SQLite 落盘、零外部中间件）根本用不了本库。
// 现在本地形态的依赖面里没有 redis。
//
// # 迁移路径（破坏性变更）
//
// Redis 后端从本包迁出到独立 module github.com/oekit/timertask/redis：
//
//	import (
//		"github.com/oekit/timertask"        // 通用类型 + LocalBackend
//		"github.com/oekit/timertask/redis"  // RedisBackend（原 timertask.NewRedisBackend）
//	)
//
//	// 旧：b := timertask.NewRedisBackend("order", client, handler)
//	// 新：b := redis.NewRedisBackend("order", client, handler)
//
// 选项函数仍留在本包（timertask.WithWorkers(4) 等），写法不变；
// 本包的 NewLocalBackend / Task / Handler / TimerTask 接口用法完全不变。
//
// # 消费语义
//
// LocalBackend 为纯内存实现，进程退出即丢失所有未处理任务，不适合承载不可丢任务。
//
// Redis 后端（github.com/oekit/timertask/redis）支持两种投递模式
// （WithDeliveryMode / WithAtLeastOnce）：
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
//   - 优雅停机（ctx 取消）时，本地队列中尚未处理的任务会回灌 Redis，
//     重启后继续消费；
//   - 重试耗尽的任务会写入持久死信 ZSet（{timertask.<buzType>}.dead，
//     score 为失败时间戳，默认保留最近 10000 条）；WithOnFailure 回调
//     只是死信之外的进程内通知手段，不构成持久化。
//
// 哪种任务选哪种模式：
//   - 提醒/审计/计费等不可丢事件 → DeliveryAtLeastOnce + 调用方幂等；
//   - 可丢弃的指标/通知/缓存预热 → DeliveryAtMostOnce（更省 Redis 往返）。
package timertask
