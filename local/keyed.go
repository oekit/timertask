package local

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	pkgerrors "github.com/oekit/pkg/errors"
	"github.com/oekit/pkg/timex/cron"

	"github.com/oekit/timertask/internal/gosafe"
	"github.com/oekit/timertask/internal/reconcile"
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
	// Fingerprint 是变更指纹（#261，diff 单元下沉 cron 同款语义）：
	//
	//   - 留空（缺省）⇒ 派生指纹 = Spec，diff 只看表达式——与历史行为
	//     逐字节兼容；
	//   - 非空 ⇒ diff 看（Spec, Fingerprint）二元组：spec 未变而指纹变化
	//     也按「被编辑过」重装（从新基准起算，zeus taskFingerprint 语义）。
	//
	// 注意指纹构成是调用方的契约选择：含 updatedAt 时「改任务元数据」
	// 也会重装、@every 触发基准随之重置（刻意语义——编辑过就该从新基准
	// 起算）；只想「行为变更才重装」就把指纹定为 表达式+状态 的哈希。
	Fingerprint string
	Task        Task
}

// deriveFingerprint 返回条目的 diff 指纹（留空派生 = Spec，见 Entry.Fingerprint
// 与 InstallFor 的指纹参数）。
func deriveFingerprint(spec, fingerprint string) string {
	if fingerprint != "" {
		return fingerprint
	}
	return spec
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
// 本函数等价 InstallFor(key, spec, "", fn)——diff 只看表达式（缺省指纹）。
// 需要「spec 未变而指纹变化也重装」的指纹语义（调度平台/动态装配形态，
// zeus taskFingerprint 同款）请用 InstallFor 或 Reconcile 的
// Entry.Fingerprint（#261：指纹 diff 单源下沉本调度器，消费方无须自持
// 登记簿镜像、无须「先摘后装」的 forced hack）。
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
// 停止时返回错误，此时该 key 的既有注册不受影响（校验先于任何变更，不存在
// 「摘了装不上」的中间态）。Start 之前调用亦可：条目先入表，Start 时随全部
// 条目一起启动。
func (s *Scheduler) ScheduleFor(key, spec string, fn Task) error {
	return s.InstallFor(key, spec, "", fn)
}

// InstallFor 是 ScheduleFor 的指纹感知安装原语（#261）：按（Spec, 派生指纹）
// 二元组做 diff——指纹未变幂等空操作（不重装、不重置触发基准），spec 或
// 指纹任一变化摘旧装新。fingerprint 留空派生 = spec（与 Entry.Fingerprint
// 同口径）：InstallFor(key, spec, "", fn) 即 ScheduleFor。
//
// 重装是**先校验后原子换装**：spec 解析/可满足性校验全部通过才动注册表，
// 失败路径的既有注册不受影响——不存在旧实现的「先摘后装、摘了装不上」
// 中间态（cron.Scheduler 的 forced hack 与手动回滚由本语义取代）。
//
// 其余语义（幂等防呆 / nil Task 口径 / 错误面 / Start 前调用）与
// ScheduleFor 一致，见其注释。
func (s *Scheduler) InstallFor(key, spec, fingerprint string, fn Task) error {
	if key == "" {
		return errors.New("local: empty key")
	}
	sched, err := s.parseSpec(spec)
	if err != nil {
		return pkgerrors.Wrapf(err, "local: parse cron %q", spec)
	}
	// 与 Schedule 同一口径（#123）：试算首个触发点，求不出即报错。
	if err := s.checkSatisfiable(sched); err != nil {
		return fmt.Errorf("%w: %q", err, spec)
	}
	fp := deriveFingerprint(spec, fingerprint)

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ErrSchedulerStopped
	}
	old := s.keyed[key]
	if old != nil && old.spec == spec && old.fingerprint == fp {
		// 指纹未变：幂等空操作（防 @every 重置，见 ScheduleFor 注释）。
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
		return ErrNilTask
	}
	if old != nil {
		delete(s.keyed, key)
	}
	e := &entry{
		spec:        spec,
		schedule:    sched,
		task:        fn,
		key:         key,
		fingerprint: fp,
		lastFire:    s.clock.Now(),
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

// InstalledKeys 返回当前已注册业务键形态条目的全部 key（排序后返回，消费方
// 断言/收敛可确定）。#261：动态装配面（cron.Scheduler）据此做「已装但不在
// 期望集」的注销收敛——登记簿单源本调度器，消费方无须自持一份镜像登记簿
// （双份登记簿对同一概念状态各持一面，diff 规则一旦分叉就出现「指纹变了
// 摘不下来」的漂移）。不含 spec 键形态的条目（两形态并存不互通）。
func (s *Scheduler) InstalledKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.keyed))
	for k := range s.keyed {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
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
//     不做任何变更并返回错误（校验块单源 internal/reconcile.ValidateDesiredSet，
//     #261——与 cron.Reconcile 同一块判定）；
//   - 收敛原子：期间并发的 ScheduleFor/UnscheduleFor 不会看到中间态
//     （全部变更在单次持锁内完成）。
//
// errors 全部成功时返回 nil；调用方通常应串行化对账（单 goroutine 周期
// 触发），与多实例并发对账同一期望集是安全的（收敛结果一致）。
func (s *Scheduler) Reconcile(entries []Entry) error {
	ids := make([]string, len(entries))
	for i := range entries {
		ids[i] = entries[i].Key
	}
	if err := reconcile.ValidateDesiredSet("local", "key", ids); err != nil {
		return err
	}
	seen := make(map[string]bool, len(entries))
	for _, id := range ids {
		seen[id] = true
	}

	// 锁外解析期望集里的 spec：只解析「需要安装/重装」的条目——指纹未变
	// 的条目是幂等空操作，注册时已校验过，无须重验。
	type install struct {
		key   string
		spec  string
		fp    string
		fn    Task
		sched cron.Schedule
	}
	installs := make([]install, 0, len(entries))
	var errs []error
	for i := range entries {
		en := entries[i]
		fp := deriveFingerprint(en.Spec, en.Fingerprint)
		s.mu.Lock()
		cur := s.keyed[en.Key]
		unchanged := cur != nil && cur.spec == en.Spec && cur.fingerprint == fp
		s.mu.Unlock()
		if unchanged {
			continue
		}
		if en.Task == nil {
			errs = append(errs, pkgerrors.Wrapf(ErrNilTask, "local: reconcile %q", en.Key))
			continue
		}
		sched, err := s.parseSpec(en.Spec)
		if err != nil {
			errs = append(errs, pkgerrors.Wrapf(err, "local: reconcile %q: parse cron %q", en.Key, en.Spec))
			continue
		}
		if err := s.checkSatisfiable(sched); err != nil {
			errs = append(errs, fmt.Errorf("%w: reconcile %q: %q", err, en.Key, en.Spec))
			continue
		}
		installs = append(installs, install{key: en.Key, spec: en.Spec, fp: fp, fn: en.Task, sched: sched})
	}

	// 单次持锁内完成全部 upsert 与移除：收敛原子，等待者看不到中间态。
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		errs = append(errs, ErrSchedulerStopped)
		return errors.Join(errs...)
	}
	started := s.started
	var closeOlds []*entry
	news := make([]*entry, 0, len(installs))
	for _, in := range installs {
		old := s.keyed[in.key]
		if old != nil && old.spec == in.spec && old.fingerprint == in.fp {
			continue // 锁内复核：并发方已装上同指纹 ⇒ 幂等空操作
		}
		if old != nil {
			delete(s.keyed, in.key)
			closeOlds = append(closeOlds, old)
		}
		e := &entry{
			spec:        in.spec,
			schedule:    in.sched,
			task:        in.fn,
			key:         in.key,
			fingerprint: in.fp,
			lastFire:    s.clock.Now(),
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
