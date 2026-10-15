// Package timertask 提供延迟任务队列门面。
//
// 定义 TimerTask 接口，支持多种后端实现：
//   - RedisBackend: 基于 Redis ZSet 的分布式延迟任务（分片管理、心跳、重试）
//   - LocalBackend: 基于内存的本地延迟任务（单实例场景）
//
// # 消费语义
//
// RedisBackend 的消费模型为 at-most-once：消费流程先通过 Lua 脚本原子删除
// ZSet 中的任务条目，再推入本地 worker 队列处理。如果进程在删除后、处理前
// 崩溃，该任务将永久丢失。应用层需自行保证幂等性。
//
// 两条兜底路径：
//   - 优雅停机（ctx 取消）时，本地队列中尚未处理的任务会回灌 Redis，
//     重启后继续消费（此路径为 at-least-once，处理前会重新投递）；
//   - 重试耗尽的任务会写入持久死信 ZSet（{timertask.<buzType>}.dead，
//     score 为失败时间戳，默认保留最近 10000 条）；WithOnFailure 回调
//     只是死信之外的进程内通知手段，不构成持久化。
//
// LocalBackend 为纯内存实现，进程退出即丢失所有未处理任务。
package timertask
