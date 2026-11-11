package timertask

import (
	"context"
	"time"
)

// RetryOutcome 表达 RunWithRetry 的三种退出形态（#169 重试主循环单源）。
type RetryOutcome int

const (
	// RetrySuccess 任务执行成功（handler 返回 nil）。
	RetrySuccess RetryOutcome = iota
	// RetryInterrupted 重试窗口内被停机打断：退避等待命中 ctx 取消/停机
	// 信号，或 Interrupted 钩子报告打断态。任务未到终态，中断处置（local
	// 后端按 at-most-once 丢弃 / redis 后端回灌或留租约）已由 OnInterrupted
	// 钩子完成。
	RetryInterrupted
	// RetryExhausted 重试次数耗尽仍失败，err 为最后一次 handler 错误。终态
	// 处置（进程内 onFailure 回调 / redis 持久死信）交给调用方。
	RetryExhausted
)

// RetryHooks 是 RunWithRetry 的观测与差异注入点（nil 钩子取缺省行为）。
//
// 两后端的差异全部留在钩子里（#169）：local 后端不注入 Interrupted /
// OnInterrupted（中断处置 = 丢弃，nil 缺省即 at-most-once 语义）；
// redis 后端注入 Interrupted（每次失败后判打断态，避免把「从未真正处理」
// 的任务写成假死信）与 OnInterrupted（handleInterruptedClaim）。
type RetryHooks struct {
	// Interrupted 在每次 handler 失败后调用：返回 true 表示消费已被停机
	// 打断，循环立即以 RetryInterrupted 结束——已取消 ctx 下 handler 必然
	// 立刻失败，继续重试只会耗尽次数、把从未真正处理的任务写进死信
	// （假死信，#151）。nil = 不检查。
	Interrupted func() bool

	// OnRetry 每次进入第 attempt 次重试前调用（attempt 从 1 起）。
	// nil = 静默。
	OnRetry func(attempt, maxRetry int)

	// OnError 每次 handler 失败后调用（含最终耗尽的那一次）。nil = 静默。
	OnError func(err error)

	// OnInterrupted 循环以 RetryInterrupted 结束时调用（退避等待被打断与
	// Interrupted 报告打断态两个出口共用）。nil = 丢弃（local 后端语义）。
	OnInterrupted func()
}

// RunWithRetry 驱动单个任务的重试主循环（#169：local 与 redis 两后端共用
// 骨架，此前平级复制两份；redis 是独立 module，读不到未导出符号，故导出）。
//
// 骨架：lazy 重试 timer（首次重试才分配，成功任务零 timer 开销）+ 退避
// select（ctx 取消 / stop 停机信号可中断）+ runHandlerSafe（panic 就地
// 收编，pkg errors.Recovered 单源）+ RetryCount 推进。后端差异由 hooks
// 注入（见 RetryHooks）。
//
// 返回退出形态与最后一次错误。task 会被改写（RetryCount 推进、MaxRetry 经
// NormalizeRetry 归一）——与两后端此前的行为一致，调用方传引擎内副本。
func RunWithRetry(ctx context.Context, task *Task, handler Handler, retryDelay time.Duration, stop <-chan struct{}, hooks RetryHooks) (RetryOutcome, error) {
	NormalizeRetry(task)
	fireInterrupted := func() {
		if hooks.OnInterrupted != nil {
			hooks.OnInterrupted()
		}
	}
	var lastErr error
	// retryTimer lazy init：i=0 直接调 handler，不需要 timer；只有真正进入
	// 重试路径才创建 timer，避免每次成功任务都起一个未消费的 timer（defer
	// Stop 不会释放已经被 runtime 调度过但未读取的 tick，长时间运行会累积）。
	// defer 在 if 块里只触发 1 次（nil-check 失败后 retryTimer != nil），
	// 不会在循环里累积，gocritic 的 deferInLoop 是误报。
	var retryTimer *time.Timer
	for i := 0; i <= task.MaxRetry; i++ {
		if i > 0 {
			// 重试延迟
			if retryTimer == nil {
				retryTimer = time.NewTimer(retryDelay)
				//nolint:gocritic // lazy init：仅首次进入重试路径时分配，defer 与 NewTimer 配对
				defer retryTimer.Stop()
			} else {
				retryTimer.Reset(retryDelay)
			}
			select {
			case <-ctx.Done():
				fireInterrupted()
				return RetryInterrupted, lastErr
			case <-stop:
				fireInterrupted()
				return RetryInterrupted, lastErr
			case <-retryTimer.C:
			}
			task.RetryCount = i
			if hooks.OnRetry != nil {
				hooks.OnRetry(i, task.MaxRetry)
			}
		}

		// handler panic 就地收编为任务失败，走重试/终态路径（见 runHandlerSafe）。
		err := runHandlerSafe(ctx, handler, task)
		if err == nil {
			return RetrySuccess, nil
		}
		lastErr = err
		if hooks.OnError != nil {
			hooks.OnError(err)
		}
		// 打断态检查（redis 后端注入；local 后端 nil 跳过，差异留在钩子里）。
		if hooks.Interrupted != nil && hooks.Interrupted() {
			fireInterrupted()
			return RetryInterrupted, lastErr
		}
	}
	return RetryExhausted, lastErr
}
