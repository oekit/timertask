package timertask

import (
	"context"
	"fmt"
	"time"
)

// 通用默认值。
//
// 这些常量原先定义在 redis.go / redis_reliable.go（Redis 后端专属文件）里，
// 拆分出嵌套模块后必须上移到根包——选项函数（WithWorkers/WithRetry/…）留在
// 根包，其默认值也必须留在根包。
const (
	// defaultWorkers 默认 worker 数量。
	defaultWorkers = 10
	// defaultRetryCount 默认重试次数。
	defaultRetryCount = 3
	// defaultRetryDelay 默认重试间隔。
	defaultRetryDelay = 5 * time.Second
	// defaultBuckets 默认分片桶数（仅 Redis 后端有效）。
	defaultBuckets = 128
	// defaultVisibilityTimeout 默认租约时长（仅 at-least-once 模式有效）。
	defaultVisibilityTimeout = 60 * time.Second
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
	// MaxRetry 最大重试次数。三态语义（此前只写「最大重试次数」，调用方
	// 按字面理解会踩坑）：
	//
	//   - == 0：**未指定** ⇒ 后端用自己的默认重试次数（NewBackend 的
	//     WithRetry 参数，默认 3）。注意这不是「不重试」——想表达「不重试」
	//     要用负数。
	//   - > 0：显式重试次数。
	//   - < 0：**不重试**（只执行一次；worker 侧归一为 0，redis 的租约回扫
	//     侧同样归一——两条路径口径一致，不会出现「一条路径判耗尽、另一条
	//     无限重投」的分叉）。
	MaxRetry int `json:"max_retry"`
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
// 支持多种后端实现：
//   - 本包 LocalBackend：基于内存的本地延迟任务（零第三方依赖，零 Redis）
//   - github.com/oekit/timertask/redis：基于 Redis ZSet 的分布式延迟任务
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

// DeliveryMode 是任务投递语义。
type DeliveryMode int

const (
	// DeliveryAtMostOnce 默认：claim 时从 ZSet 原子删除后处理。
	// 删除后、处理前崩溃 = 任务永久丢失（仅优雅停机回灌兜底）。
	DeliveryAtMostOnce DeliveryMode = iota
	// DeliveryAtLeastOnce 可靠模式：claim 时移入 processing 租约集，
	// handler 成功后 Ack 删除；租约超时由回扫重投。可容忍重复，调用方幂等。
	DeliveryAtLeastOnce
)

// config 是延迟任务的通用配置。
type config struct {
	workers           int
	retryCount        int
	retryDelay        time.Duration
	buckets           int                // 仅 Redis 模式有效
	onFailure         func(*Task, error) // 任务最终失败回调（通知用途；持久死信由 RedisBackend 自动写入）
	delivery          DeliveryMode
	visibilityTimeout time.Duration // 仅 DeliveryAtLeastOnce：processing 租约时长
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

// WithBuckets 设置任务桶数量（仅 Redis 后端 github.com/oekit/timertask/redis 有效），默认 128。
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
// 不是持久死信——Redis 后端（github.com/oekit/timertask/redis）会自动把最终
// 失败的任务写入 Redis 死信 ZSet，LocalBackend 没有持久死信。
// 默认仅记录日志。
func WithOnFailure(fn func(task *Task, err error)) TimerTaskOption {
	return func(c *config) {
		c.onFailure = fn
	}
}

// WithDeliveryMode 设置投递语义。默认 DeliveryAtMostOnce（与历史行为一致）。
// DeliveryAtLeastOnce 时可用 WithVisibilityTimeout 调整租约时长。
// 仅 Redis 后端（github.com/oekit/timertask/redis）生效；LocalBackend 为纯内存，
// 无持久投递语义。
func WithDeliveryMode(m DeliveryMode) TimerTaskOption {
	return func(c *config) { c.delivery = m }
}

// WithAtLeastOnce 开启可靠投递（等价 WithDeliveryMode(DeliveryAtLeastOnce)），
// visibilityTimeout 为 processing 租约时长（<=0 取默认 60s）。
// claim 后任务进入租约集，成功 Ack 删除；超时未 Ack 由回扫按 RetryCount 重投。
func WithAtLeastOnce(visibilityTimeout time.Duration) TimerTaskOption {
	return func(c *config) {
		c.delivery = DeliveryAtLeastOnce
		if visibilityTimeout > 0 {
			c.visibilityTimeout = visibilityTimeout
		}
	}
}

// WithVisibilityTimeout 设置 at-least-once 模式的租约时长（<=0 取默认 60s）。
func WithVisibilityTimeout(d time.Duration) TimerTaskOption {
	return func(c *config) {
		if d > 0 {
			c.visibilityTimeout = d
		}
	}
}

// applyOptions 应用选项并返回配置。
func applyOptions(opts []TimerTaskOption) config {
	cfg := config{
		workers:           defaultWorkers,
		retryCount:        defaultRetryCount,
		retryDelay:        defaultRetryDelay,
		buckets:           defaultBuckets,
		visibilityTimeout: defaultVisibilityTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// BackendConfig 是选项解析后的后端配置。
//
// 存在原因：TimerTaskOption 的底层类型 config 是未导出的，嵌套后端模块
// （github.com/oekit/timertask/redis，独立 module）无法直接解析选项。
// 通过本结构把解析结果交给嵌套模块，下游调用方只需改 import 路径，
// 选项写法（timertask.WithWorkers(4) 等）保持不变。
type BackendConfig struct {
	// Workers worker 数量。
	Workers int
	// RetryCount 任务重试次数。
	RetryCount int
	// RetryDelay 任务重试间隔。
	RetryDelay time.Duration
	// Buckets 分片桶数（仅 Redis 后端有效，本地后端忽略）。
	Buckets int
	// OnFailure 任务最终失败回调（可为 nil）。
	OnFailure func(*Task, error)
	// Delivery 投递语义。
	Delivery DeliveryMode
	// VisibilityTimeout at-least-once 模式的租约时长。
	VisibilityTimeout time.Duration
}

// ResolveOptions 解析选项并返回后端配置。
//
// 供嵌套后端模块使用；普通调用方用 NewLocalBackend 等构造函数即可，
// 无需直接调用本函数。
func ResolveOptions(opts ...TimerTaskOption) BackendConfig {
	cfg := applyOptions(opts)
	return BackendConfig{
		Workers:           cfg.workers,
		RetryCount:        cfg.retryCount,
		RetryDelay:        cfg.retryDelay,
		Buckets:           cfg.buckets,
		OnFailure:         cfg.onFailure,
		Delivery:          cfg.delivery,
		VisibilityTimeout: cfg.visibilityTimeout,
	}
}
