// Package timertask 提供延迟任务队列门面。
//
// 定义 TimerTask 接口，支持多种后端实现：
//   - RedisBackend: 基于 Redis ZSet 的分布式延迟任务（分片管理、心跳、重试）
//   - LocalBackend: 基于内存的本地延迟任务（单实例场景）
//
// # 消费语义
//
// RedisBackend 支持两种投递模式（WithDeliveryMode / WithAtLeastOnce）：
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
//
// LocalBackend 为纯内存实现，进程退出即丢失所有未处理任务，不适合承载不可丢任务。
package timertask
