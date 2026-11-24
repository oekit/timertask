package timertask

import (
	"sync"
	"time"
)

// DefaultShutdownDrainGrace 是停机时等 in-flight worker 到任务边界的有界
// 宽限（#242），Local/Redis 两后端共用（#262 单源）：宽限内 worker 在任务
// 边界退出（stopChan 已关）；超窗按各后端语义收尾——Local 直接停机
// （at-most-once 不回灌），Redis 继续回灌（进程退出后租约由 TTL 兜底）。
//
// 此前两后端各持一份 10s 常量（shutdownDrainGraceLocal / shutdownDrainGrace），
// 值漂移会让两后端的停机预算悄然分叉；根包已为 #169 导出哨兵与
// RunWithRetry，常量同面单源。
const DefaultShutdownDrainGrace = 10 * time.Second

// StartState 是后端 Start 生命周期状态机（#124）的共享检查面（#262 单源）：
// Local/Redis 两后端的「三态检查 + 错误映射」收敛到根包——停机态
// ErrBackendStopped、已启动 ErrBackendAlreadyStarted、未启动登记 started
// 放行，判定顺序与错误身份此前在两后端逐行复制。
//
// 互斥由各后端自持（字段不代持锁）：stopped 检查必须并入各后端自己的
// 临界段——LocalBackend.Send 的 stopped 检查与查重入堆同段、RedisBackend
// 的检查与停机登记同锁——方法名以 Locked 结尾的约定调用方已持有后端锁。
type StartState struct {
	started  bool
	stopping bool
	stopped  bool
}

// BeginStartLocked 是 Start 入口的三态检查（#124，调用方须持有后端锁）：
// 停机态返回 ErrBackendStopped（实例一次性，Start 返回后终态）；已启动返回
// ErrBackendAlreadyStarted（首个 Start 尚未返回时的并发重复形态）；否则登记
// started 并返回 nil。检查必须在拉起消费 goroutine 之前，否则二次调用同样
// 会拉起一组必然哑火的消费者。
func (st *StartState) BeginStartLocked() error {
	if st.stopped {
		return ErrBackendStopped
	}
	if st.started {
		return ErrBackendAlreadyStarted
	}
	st.started = true
	return nil
}

// StoppedLocked 报告后端是否已不再接受新任务（调用方须持有后端锁）。
// 两个阶段都返回 true（#292）：
//
//   - stopping：Start 已收到停机信号并在同一临界段关闭 stopChan——消费
//     goroutine（scanLoop/consumeRemoteLoop）已经或即将退出，此后入队的任务
//     不会再被消费。停机排水/回灌可能还要跑一段时间（in-flight worker 宽限、
//     回灌窗口），窗口内 Send 必须立即拒绝，否则调用方拿到「提交成功」、
//     任务只留在内存堆里，停机路径不处理堆内任务 ⇒ 永久丢失；
//   - stopped：Start 已返回，实例进入终态。
//
// Send 等「停机后拒绝新任务」（#124）的入口与 Start 的停机登记共用本判定。
func (st *StartState) StoppedLocked() bool {
	return st.stopping || st.stopped
}

// MarkStoppingLocked 登记「停机进行中」（调用方须持有后端锁）：Start 收到
// 停机信号后、关闭 stopChan 的同一临界段调用（#292）。此后的 Send 立即返回
// ErrBackendStopped，不再把任务压入无人消费的内存堆。
func (st *StartState) MarkStoppingLocked() {
	st.stopping = true
}

// MarkStoppedLocked 登记停机终态（调用方须持有后端锁）：Start 的停机收尾
// 完成后调用——此后的二次 Start / Send 都显式报错（ErrBackendStopped）。
func (st *StartState) MarkStoppedLocked() {
	st.stopped = true
}

// DrainWorkers 等待 in-flight worker 到任务边界（#242）：等 wg 全部 Done
// 或宽限超窗，排空返回 true、超窗返回 false。调用方在 close(stopChan)
// 之后调用；超窗的收尾语义由各后端决定（Local 直接停机 / Redis 记警告后
// 继续回灌）。
//
// #262 单源：两后端的「goroutine Wait + time.After 有界宽限」停机排水
// 骨架收敛于此，宽限预算传 DefaultShutdownDrainGrace（或测试自定值）。
func DrainWorkers(wg *sync.WaitGroup, grace time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		return false
	}
}
