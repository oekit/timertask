package timertask

import (
	"context"
	"fmt"
	"time"
)

// Task 是一个延迟任务。
type Task struct {
	ID         string `json:"task_id"`     // 任务唯一 ID
	CreateTime int64  `json:"create_time"` // 创建时间（Unix 秒）
	EndTime    int64  `json:"end_time"`    // 执行时间（Unix 秒）
	Action     string `json:"action"`      // 任务类型/动作名
	Param      string `json:"param"`       // 业务参数（JSON 字符串）
	Extra      string `json:"extra"`       // 扩展字段
	RetryCount int    `json:"retry_count"` // 已重试次数
	MaxRetry   int    `json:"max_retry"`   // 最大重试次数
}

// Handler 是任务处理函数。
type Handler func(ctx context.Context, task *Task) error

// runHandlerSafe 就地收编 handler panic 为任务失败：async.GoSafe 的 recover 在
// goroutine 出口，panic 会直接打死 worker，N 个 panic 任务耗尽整个 worker 池后
// 队列塞满、消费停滞，整个后端瘫痪。收编后 panic 任务走既有重试/死信路径。
func runHandlerSafe(ctx context.Context, handler Handler, task *Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return handler(ctx, task)
}

// normalizeRetry 把非法的负 MaxRetry 归一为 0（至少执行一次、不重试），
// 避免"未执行即进死信"的假死信。
func normalizeRetry(task *Task) {
	if task.MaxRetry < 0 {
		task.MaxRetry = 0
	}
}

// TimerTask 是延迟任务队列接口。
// 支持多种后端实现（Redis、本地内存等）。
type TimerTask interface {
	// Send 提交一个延迟任务。
	// task.EndTime（Unix 秒）决定任务何时被执行。
	Send(ctx context.Context, task *Task) error

	// Start 启动任务消费（阻塞）。
	// ctx 取消时所有 goroutine 退出。
	Start(ctx context.Context) error

	// PendingCount 返回队列中待处理任务数。
	PendingCount(ctx context.Context) (int64, error)

	// LocalPendingCount 返回本地队列中待处理任务数。
	LocalPendingCount() int
}

// config 是延迟任务的通用配置。
type config struct {
	workers    int
	retryCount int
	retryDelay time.Duration
	buckets    int                // 仅 Redis 模式有效
	onFailure  func(*Task, error) // 任务最终失败回调（通知用途；持久死信由 RedisBackend 自动写入）
}

// TimerTaskOption 配置延迟任务的可选参数（函数选项模式）。
type TimerTaskOption func(*config)

// WithRetry 设置任务重试次数和延迟，默认 3 次，5 秒延迟。
func WithRetry(count int, delay time.Duration) TimerTaskOption {
	return func(c *config) {
		c.retryCount = count
		c.retryDelay = delay
	}
}

// WithBuckets 设置任务桶数量（仅 Redis 模式有效），默认 128。
func WithBuckets(n int) TimerTaskOption {
	return func(c *config) {
		if n < 2 {
			n = 2
		}
		if n > 1024 {
			n = 1024
		}
		c.buckets = n
	}
}

// WithWorkers 设置 Worker 数量，默认 10。
func WithWorkers(n int) TimerTaskOption {
	return func(c *config) {
		if n < 1 {
			n = 1
		}
		c.workers = n
	}
}

// WithOnFailure 设置任务最终失败回调。
// 当任务重试次数用完后调用，可用于告警、埋点等；注意这是进程内的同步回调，
// 不是持久死信——RedisBackend 会自动把最终失败的任务写入 Redis 死信 ZSet，
// LocalBackend 没有持久死信。
// 默认仅记录日志。
func WithOnFailure(fn func(task *Task, err error)) TimerTaskOption {
	return func(c *config) {
		c.onFailure = fn
	}
}

// applyOptions 应用选项并返回配置。
func applyOptions(opts []TimerTaskOption) config {
	cfg := config{
		workers:    defaultWorkers,
		retryCount: defaultRetryCount,
		retryDelay: defaultRetryDelay,
		buckets:    defaultBuckets,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
