package cron

import "time"

// 默认重试预算：对齐根包延迟任务的默认量级（WithRetry 默认 3 次 / 5s 间隔、
// WithVisibilityTimeout 默认 60s 租约），沿用同一族默认值让「锁 TTL 预算」
// 与「任务执行预算」在同一量纲上起步。
const (
	// DefaultBudgetAttempts 默认总尝试次数（含首次）。
	DefaultBudgetAttempts = 3
	// DefaultBudgetTimeout 默认单次尝试上限。
	DefaultBudgetTimeout = 60 * time.Second
	// DefaultBudgetBackoff 默认相邻尝试间退避。
	DefaultBudgetBackoff = 5 * time.Second
	// DefaultBudgetSlack 默认余量（时钟漂移/调度延迟）。
	DefaultBudgetSlack = 30 * time.Second
)

// RetryBudget 是任务执行的重试预算，也是**互斥锁 TTL 预算公式的唯一出处**
// （#159：与 zeus/task 的 execLockTTL 同口径收编）：
//
//	锁 TTL = 全部尝试 + 全部退避 + 余量
//	       = Attempts*Timeout + TotalBackoff() + Slack
//
// 此前该预算散在下游各自手算，口径漂移的后果是互斥提前过期——同 slot 被
// 他实例接管、双跑破坏「触发点恰好一次」。公式必须单点：改预算只改这里。
//
// 零值可用（Normalized 归一：Attempts < 1 取 1、负时长归 0、Slack <= 0 取
// DefaultBudgetSlack——零值透传的 Slack 才是缺省，不是「不要余量」；确需
// 零余量显式给 1ns 这类正值）。
type RetryBudget struct {
	// Attempts 总尝试次数（含首次），<1 归一为 1。
	Attempts int
	// Timeout 单次尝试（执行 + 尝试内等待）上限，<=0 归 0。
	Timeout time.Duration
	// Backoff 相邻尝试间的固定退避，<=0 归 0。
	Backoff time.Duration
	// Slack 余量（时钟漂移/调度延迟），<=0 取 DefaultBudgetSlack。
	Slack time.Duration
}

// Normalized 返回归一后的预算（负值钳制 min/max 内建；Slack 零值/负值取
// DefaultBudgetSlack——显式更小的正值照用）。
func (b RetryBudget) Normalized() RetryBudget {
	b.Attempts = max(1, b.Attempts)
	b.Timeout = max(0, b.Timeout)
	b.Backoff = max(0, b.Backoff)
	if b.Slack <= 0 {
		b.Slack = DefaultBudgetSlack
	}
	return b
}

// TotalBackoff 返回全部退避之和 = (Attempts-1)*Backoff（首次与末次之间
// 共 Attempts-1 个退避间隔）。
func (b RetryBudget) TotalBackoff() time.Duration {
	n := b.Normalized()
	return time.Duration(n.Attempts-1) * n.Backoff
}

// LockTTL 返回互斥锁的租约时长（唯一出处，见类型注释）：
// Attempts*Timeout + TotalBackoff() + Slack。
func (b RetryBudget) LockTTL() time.Duration {
	n := b.Normalized()
	return time.Duration(n.Attempts)*n.Timeout + n.TotalBackoff() + n.Slack
}
