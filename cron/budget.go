package cron

import (
	"math"
	"time"
)

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
// 公式的两侧同源（#292）：fireAttempt 的执行路径也按本预算走——每次尝试
// 受 Timeout 约束（context.WithTimeout）、一共 Attempts 次、尝试之间按
// Backoff 退避。四项都不是「只给 LockTTL 看的文档字段」：调大 Timeout 会
// 真的放宽单次执行时限，也会同步抬 LockTTL，租约与执行不会分叉。
//
// 零值可用（Normalized 归一：Attempts < 1 取 1、Timeout <= 0 取
// DefaultBudgetTimeout、Backoff 负值归 0、Slack <= 0 取 DefaultBudgetSlack
// ——零值透传的才是缺省，不是「不要」；确需零余量显式给 1ns 这类正值）。
//
// ⚠️ Timeout 零值=缺省的口径（timertask#244）：此前 "<=0 归 0 = 执行无时限"
// 与 LockTTL 公式矛盾——`WithRetryBudget(RetryBudget{Attempts: 5})`（文档
// 邀请的零值用法）算出 LockTTL = 5*0 + 0 + 30s = 30s，任务跑过 30s 租约即
// 过期、他实例接管 ⇒ **同一触发点双跑**，恰好击穿本公式「互斥不过期」的
// 立项理由。执行时限与租约必须同源，零值统一取缺省（60s/次）。
type RetryBudget struct {
	// Attempts 总尝试次数（含首次），<1 归一为 1。
	Attempts int
	// Timeout 单次尝试（执行 + 尝试内等待）上限，<=0 取 DefaultBudgetTimeout
	// （零值=缺省，timertask#244——0=无时限曾让 LockTTL 只剩 Slack、租约
	// 先于执行过期致双跑）。
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
	// timertask#244：<=0（零值与显式非正）取缺省——与 Slack 同口径。
	if b.Timeout <= 0 {
		b.Timeout = DefaultBudgetTimeout
	}
	b.Backoff = max(0, b.Backoff)
	if b.Slack <= 0 {
		b.Slack = DefaultBudgetSlack
	}
	return b
}

// mulSaturate 计算 attempts*unit，**乘前判溢出**（timertask#285，对齐
// concurrency/retry #283 的饱和哲学）：线性裸乘对配置无界，超大配置回绕成
// 负 duration——负退避静默消灭防线，负租约还会被 LockTTL 的 #244 防御
// panic 误报成「公式破坏」。即将越过 MaxInt64 即饱和到 MaxInt64
// （≈292 年，远超任何真实预算），不回绕为负。
func mulSaturate(attempts int, unit time.Duration) time.Duration {
	if attempts <= 0 || unit <= 0 {
		return 0
	}
	if int64(attempts) > math.MaxInt64/int64(unit) {
		return math.MaxInt64
	}
	return time.Duration(attempts) * unit
}

// addSaturate 两段时长求和，和溢出饱和到 MaxInt64，不回绕为负
// （timertask#285：与 mulSaturate 同一哲学，对齐 retry.TotalBackoff 的
// 求和口径——三段预算相加同样可能越过 MaxInt64）。
func addSaturate(a, b time.Duration) time.Duration {
	if a > 0 && b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// TotalBackoff 返回全部退避之和 = (Attempts-1)*Backoff（首次与末次之间
// 共 Attempts-1 个退避间隔）。乘前判溢出，饱和而非回绕（#285）。
func (b RetryBudget) TotalBackoff() time.Duration {
	n := b.Normalized()
	return mulSaturate(n.Attempts-1, n.Backoff)
}

// LockTTL 返回互斥锁的租约时长（唯一出处，见类型注释）：
// Attempts*Timeout + TotalBackoff() + Slack。各项乘/加均判溢出、饱和到
// MaxInt64 而非回绕（#285）——饱和租约仍满足「租约不先于执行过期」的
// 公式承诺，回绕成负则租约立即过期、双跑。
func (b RetryBudget) LockTTL() time.Duration {
	n := b.Normalized()
	ttl := addSaturate(
		mulSaturate(n.Attempts, n.Timeout),
		addSaturate(n.TotalBackoff(), n.Slack),
	)
	if ttl <= 0 {
		// timertask#244 防御：归一化后公式必为正（Attempts>=1、Timeout/
		// Slack 有正缺省）——ttl<=0 说明公式被后续改动破坏，而 0/负租约
		// = 租约立即过期 = 双跑，fail fast 好过静默放过。（#285 起溢出
		// 走饱和不再产生负 ttl，此护栏只剩「公式被改坏」一种触发方式。）
		panic("timertask: RetryBudget.LockTTL non-positive after normalize — formula broken")
	}
	return ttl
}
