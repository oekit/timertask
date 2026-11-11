package local

import (
	"errors"
	"fmt"
	"sync"

	"github.com/oekit/pkg/timex/cron"

	"github.com/oekit/timertask/internal/gosafe"
)

// Entry 是业务键形态的期望条目（Reconcile 的输入，#129）。
//
// Key 是业务侧的任务标识（任务 ID），同一调度器内唯一；Spec 是 cron 表达式
// （与 Schedule 同口径：5/6 字段 + @every 等宏）；Task 在安装新 key / 重装
// 变更的 key 时使用，条目 spec 未变（幂等空操作）时被忽略——对账只需保
// spec 与触发行为，任务函数以首次注册为准。
type Entry struct {
	Key  string
	Spec string
	Task Task
}

// ScheduleFor 按业务键 upsert 一个 cron 任务（#129）。
//
// 与 spec 键形态（Schedule/Unschedule，键即表达式、重复注册 ErrDuplicateSpec）
// 并存不互通，面向「调度平台」的动态形态：按任务 ID 管理条目，任务改表达式
// 时同步调度计划，调用方无须自己维护 spec→taskID 映射。
//
// 语义（指纹幂等，zeus 实证的同款防呆）：
//
//   - key 未注册 ⇒ 安装（Task 必填）；
//   - key 已注册且 spec 未变 ⇒ **幂等空操作**——不重装、不重置触发基准。
//     重装会把 entry 的 lastFire 重置到此刻：@every 解析为
//     ConstantDelaySchedule（相对基准的固定间隔），「周期性重装」会把间隔
//     长于重装周期的任务的下次触发时间不断往后推——任务永不执行。因此
//     **换任务函数（Task 不可比）请先 UnscheduleFor 再 ScheduleFor**，
//     同 spec 重复调用本函数不会生效；
//   - key 已注册且 spec 变化 ⇒ 摘旧装新（Task 必填）：旧表达式立即停止
//     触发（正在执行的旧任务不打断，与 Unschedule 语义一致），新表达式从
//     注册时刻起算。
//
// Task 与 Reconcile 同口径（#155）：仅在「需要安装/重装」时使用，spec 未变
// 的幂等空操作忽略 Task（fn 为 nil 亦放行，任务函数以首次注册为准）——纯
// 表达式 upsert（ScheduleFor(key, spec, nil)）由此可用；需要安装/重装而
// fn 为 nil 时返回错误。
//
// 同一 spec 可挂多个业务键（两个任务共用同一表达式，各自触发）；key 键与
// spec 键互不可见——同一 spec 在两种形态下各注册一次会各自触发一次，混用
// 前请确认这正是想要的。
//
// spec 解析失败 / 语法合法但永无触发点（ErrUnsatisfiableSpec）/ 调度器已
// 停止时返回错误，此时该 key 的既有注册不受影响。Start 之前调用亦可：
// 条目先入表，Start 时随全部条目一起启动。
func (s *Scheduler) ScheduleFor(key, spec string, fn Task) error {
	if key == "" {
		return errors.New("local: empty key")
	}
	sched, err := s.parseSpec(spec)
	if err != nil {
		return fmt.Errorf("local: parse cron %q: %w", spec, err)
	}
	// 与 Schedule 同一口径（#123）：试算首个触发点，求不出即报错。
	if err := s.checkSatisfiable(sched); err != nil {
		return fmt.Errorf("%w: %q", err, spec)
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return errors.New("local: scheduler stopped")
	}
	old := s.keyed[key]
	if old != nil && old.spec == spec {
		// 指纹（spec）未变：幂等空操作（防 @every 重置，见函数注释）。
		// Task 此时被忽略（nil 亦然，#155——与 Reconcile 的 unchanged 分支
		// 同口径）。
		s.mu.Unlock()
		return nil
	}
	// nil 检查移入「需要安装/重装」分支（#155，与 Reconcile 的安装分支同序）：
	// 全局前置检查会把「spec 未变、Task 以首次注册为准」的纯表达式 upsert
	// 一并拒掉，与 Reconcile「spec 未变时 Task 被忽略」口径分裂。
	if fn == nil {
		s.mu.Unlock()
		return errors.New("local: nil task")
	}
	if old != nil {
		delete(s.keyed, key)
	}
	e := &entry{
		spec:     spec,
		schedule: sched,
		task:     fn,
		key:      key,
		lastFire: s.clock.Now(),
	}
	e.cond = sync.NewCond(&e.mu)
	s.keyed[key] = e
	started := s.started
	if started {
		s.addRunnerLocked()
	}
	s.mu.Unlock()

	if old != nil {
		old.close() // 旧 spec 的 runner 退出、未开始的触发丢弃
	}
	if started {
		gosafe.Go(func() { s.runEntry(e) })
	}
	s.notify()
	return nil
}

// UnscheduleFor 按业务键注销一个 cron 任务，返回是否确实移除了该任务。
// 语义与 Unschedule 一致：正在执行的任务不会被中断（任务体可自行感知
// ctx 取消）；未开始的触发被丢弃。
func (s *Scheduler) UnscheduleFor(key string) bool {
	s.mu.Lock()
	e, ok := s.keyed[key]
	if ok {
		delete(s.keyed, key)
	}
	s.mu.Unlock()
	if !ok {
		return false
	}
	e.close()
	s.notify()
	return true
}

// Reconcile 把业务键形态的任务收敛到期望集（#129）：期望集里的条目按
// ScheduleFor 语义增量 upsert（spec 变化重装、未变幂等空操作），已注册但
// 不在期望集里的 key 注销。多实例共用同一 DB 期望集、周期对账的场景下，
// 期望集未变时 Reconcile 是**空操作**（不重装、不重置 @every 触发基准）。
//
// 对账语义（与调度平台消费方对齐）：
//
//   - 启停 / 变更的表达式由期望集表达：启用的任务出现在期望集里，停用的
//     不出现（自动注销）——调用方无须 diff，库侧收敛；
//   - 单条 spec 解析失败 / 不可满足**不阻断**其余条目的收敛（一个坏任务
//     不应阻止其余任务对账），失败逐条汇总进返回错误（errors.Join）；
//   - 期望集内 key 重复 ⇒ 「以哪条为准」存在歧义，fail-closed：本次调用
//     不做任何变更并返回错误；
//   - 收敛原子：期间并发的 ScheduleFor/UnscheduleFor 不会看到中间态
//     （全部变更在单次持锁内完成）。
//
// errors 全部成功时返回 nil；调用方通常应串行化对账（单 goroutine 周期
// 触发），与多实例并发对账同一期望集是安全的（收敛结果一致）。
func (s *Scheduler) Reconcile(entries []Entry) error {
	seen := make(map[string]bool, len(entries))
	for i := range entries {
		if entries[i].Key == "" {
			return errors.New("local: reconcile: empty key in desired set")
		}
		if seen[entries[i].Key] {
			return fmt.Errorf("local: reconcile: duplicate key %q in desired set", entries[i].Key)
		}
		seen[entries[i].Key] = true
	}

	// 锁外解析期望集里的 spec：只解析「需要安装/重装」的条目——spec 未变
	// 的条目是幂等空操作，注册时已校验过，无须重验。
	type install struct {
		key   string
		spec  string
		fn    Task
		sched cron.Schedule
	}
	installs := make([]install, 0, len(entries))
	var errs []error
	for i := range entries {
		en := entries[i]
		s.mu.Lock()
		cur := s.keyed[en.Key]
		unchanged := cur != nil && cur.spec == en.Spec
		s.mu.Unlock()
		if unchanged {
			continue
		}
		if en.Task == nil {
			errs = append(errs, fmt.Errorf("local: reconcile %q: nil task", en.Key))
			continue
		}
		sched, err := s.parseSpec(en.Spec)
		if err != nil {
			errs = append(errs, fmt.Errorf("local: reconcile %q: parse cron %q: %w", en.Key, en.Spec, err))
			continue
		}
		if err := s.checkSatisfiable(sched); err != nil {
			errs = append(errs, fmt.Errorf("%w: reconcile %q: %q", err, en.Key, en.Spec))
			continue
		}
		installs = append(installs, install{key: en.Key, spec: en.Spec, fn: en.Task, sched: sched})
	}

	// 单次持锁内完成全部 upsert 与移除：收敛原子，等待者看不到中间态。
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		errs = append(errs, errors.New("local: scheduler stopped"))
		return errors.Join(errs...)
	}
	started := s.started
	var closeOlds []*entry
	news := make([]*entry, 0, len(installs))
	for _, in := range installs {
		old := s.keyed[in.key]
		if old != nil && old.spec == in.spec {
			continue // 锁内复核：并发方已装上同 spec ⇒ 幂等空操作
		}
		if old != nil {
			delete(s.keyed, in.key)
			closeOlds = append(closeOlds, old)
		}
		e := &entry{
			spec:     in.spec,
			schedule: in.sched,
			task:     in.fn,
			key:      in.key,
			lastFire: s.clock.Now(),
		}
		e.cond = sync.NewCond(&e.mu)
		s.keyed[in.key] = e
		news = append(news, e)
		if started {
			s.addRunnerLocked()
		}
	}
	var vanished []*entry
	for key, e := range s.keyed {
		if !seen[key] {
			vanished = append(vanished, e)
			delete(s.keyed, key)
		}
	}
	s.mu.Unlock()

	for _, e := range closeOlds {
		e.close()
	}
	for _, e := range vanished {
		e.close()
	}
	if started {
		for _, e := range news {
			gosafe.Go(func() { s.runEntry(e) })
		}
	}
	if len(news) > 0 || len(vanished) > 0 {
		s.notify()
	}
	return errors.Join(errs...)
}
