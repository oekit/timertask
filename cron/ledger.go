package cron

import (
	"context"
	"sync"
	"time"
)

// DefaultRunCapacity 是内置运行台账（MemLedger）的默认容量（有界环形，
// 超容最旧记录被覆盖——台账是可观测面不是持久化，持久化走 WithLedger）。
const DefaultRunCapacity = 256

// RunRecord 是一次触发的运行台账记录（#159「运行台账：执行历史/结果可观测」）。
//
// 状态读法（Skipped/Err 组合语义）：
//
//   - Skipped=false、Err=nil     ⇒ 执行成功；
//   - Skipped=false、Err!=nil    ⇒ 执行失败（含 executor panic 收编）；
//   - Skipped=true、Err=nil      ⇒ 互斥让位（同触发点由他实例执行，跨实例
//     恰好一次的正常形态）；
//   - Skipped=true、Err!=nil     ⇒ 互斥设施故障，fail-closed 未执行（告警面）。
type RunRecord struct {
	// PlanID 计划 ID。
	PlanID string
	// Trigger 触发点标识（RFC3339 秒精度，与互斥键同源——幂等键/互斥键/
	// 台账键三同源）。
	Trigger string
	// Instance 执行实例标识（WithInstance，缺省主机名）——多实例归因。
	Instance string
	// StartedAt 执行开始时刻（跳过记录为跳过判定时刻）。
	StartedAt time.Time
	// EndedAt 执行结束时刻（跳过记录同 StartedAt）。
	EndedAt time.Time
	// Err 执行结果（nil=成功），语义见类型注释。
	Err error
	// Skipped 本次触发是否未执行（互斥让位/故障 fail-closed）。
	Skipped bool
}

// Ledger 是运行台账的最小契约（外置落库缝：DB 表/审计流等）。
//
// Record 不返回错误：台账写失败不得影响主链路（外置实现自行兜底重试）；
// Scheduler 侧对 panic 也做收编（台账故障不得打死触发）。每次触发一条
// 记录（成功/失败/让位都记——「谁在什么时候跑了哪一档、结果如何」是排障
// 与「恰好一次」核对面）。
type Ledger interface {
	Record(ctx context.Context, rec RunRecord)
}

// MemLedger 是内置有界运行台账（环形覆盖，New 缺省挂载）：RunHistory 的
// 读取面，永不无界增长。
type MemLedger struct {
	mu   sync.Mutex
	ring []RunRecord
	head int // 下一个写入位
	size int
}

// NewMemLedger 创建容量为 capacity 的有界台账（<=0 取 DefaultRunCapacity）。
func NewMemLedger(capacity int) *MemLedger {
	if capacity <= 0 {
		capacity = DefaultRunCapacity
	}
	return &MemLedger{ring: make([]RunRecord, capacity)}
}

// Record 实现 Ledger（环形覆盖最旧记录）。
func (m *MemLedger) Record(_ context.Context, rec RunRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ring[m.head] = rec
	m.head = (m.head + 1) % len(m.ring)
	if m.size < len(m.ring) {
		m.size++
	}
}

// Recent 返回全部记录（时间序旧→新）。
func (m *MemLedger) Recent() []RunRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RunRecord, 0, m.size)
	// for range int：环形回卷拷贝，m.size 是计数不是可迭代集合。
	for i := range m.size {
		out = append(out, m.ring[(m.head-m.size+i+len(m.ring))%len(m.ring)])
	}
	return out
}

// RecentFor 返回指定计划的记录（时间序旧→新）。
func (m *MemLedger) RecentFor(planID string) []RunRecord {
	all := m.Recent()
	out := make([]RunRecord, 0, len(all))
	for _, rec := range all {
		if rec.PlanID == planID {
			out = append(out, rec)
		}
	}
	return out
}
