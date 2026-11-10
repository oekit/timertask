package cron

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/timertask/local"
)

// ErrLockNotAcquired 表示锁被其他实例持有（非阻塞争抢失败）。
// Mutex.TryLock 的占用哨兵：errors.Is 判定；与「互斥设施故障」（其它 error）
// 区分——占用是常态（跳过本次触发即可），故障要进台账告警。
//
// 适配 redisx/distlock 时把 distlock.ErrLockNotAcquired 映射到本哨兵
// （见 Mutex 的 godoc 适配示例）。
var ErrLockNotAcquired = errors.New("cron: lock not acquired")

// Mutex 是多实例执行互斥的最小契约（#159）：**可选依赖 distlock 的缝**——
// 仓内不引 distlock/redisx 依赖（根模块 require 只允许 pkg），分布式锁
// 实现经本接口注入。语义与仓内 at-least-once 租约同型（visibility timeout
// 租约集）：持有即租约，TTL 到期自动可被他实例接管（崩溃实例不尸占），
// Unlock 提前释放。
//
// 执行互斥按「slot 认领」使用（fire 的消费形态）：触发点的租约**不**在
// 执行完成时提前释放，认领标记保留到 TTL 到期——同触发点的迟到重复触发
// 在租约有效期内一律让位（「触发点恰好一次」），执行完成即放锁会让迟到
// 方重跑同一 slot。确需提前释放的消费方（长租约省存储）仍可用 Unlock，
// 但那会重新打开「迟到触发重跑」的口子。
//
// TryLock/Unlock 与 redisx/distlock.Locker 同形（其 Lock 阻塞形态调度面
// 用不上，不进本契约）——*distlock.RedisLocker 结构上即满足本接口的
// 方法集，但占用哨兵不同名，语义适配包一层即可：
//
//	type distAdapter struct{ l distlock.Locker }
//
//	func (a distAdapter) TryLock(ctx context.Context, key string, ttl time.Duration) (string, error) {
//	    id, err := a.l.TryLock(ctx, key, ttl)
//	    if errors.Is(err, distlock.ErrLockNotAcquired) {
//	        return "", cron.ErrLockNotAcquired
//	    }
//	    return id, err
//	}
//
//	func (a distAdapter) Unlock(ctx context.Context, key, owner string) error {
//	    return a.l.Unlock(ctx, key, owner)
//	}
type Mutex interface {
	// TryLock 非阻塞争抢 key 的执行租约，ttl 为租约时长。
	//
	//   - 成功 ⇒ (ownerID, nil)，ownerID 非空（后续 Unlock 凭它释放）；
	//   - 被占用 ⇒ ("", ErrLockNotAcquired)（errors.Is 判定）；
	//   - 其它 error ⇒ 互斥设施故障（调度面 fail-closed 跳过本次执行，
	//     台账记错——宁可漏跑一轮（可补发）不可多实例双跑）。
	TryLock(ctx context.Context, key string, ttl time.Duration) (ownerID string, err error)

	// Unlock 释放 key 的租约，ownerID 必须匹配。锁不存在或已易主（TTL 过期
	// 后被接管）按各自后端语义返回错误——调度面只记日志不改执行结果
	// （执行已结束，租约易主是租约模型的正常演进）。
	Unlock(ctx context.Context, key string, ownerID string) error
}

// execLockKey 是执行互斥的锁键格式：`timertask/cron/exec/<planID>/<trigger>`。
// 契约面（distlock 侧排障/配额按前缀识别）：**稳定不漂移**，trigger 为
// 触发点标识（RFC3339，秒精度）——同一触发点跨实例同键，互斥裁决恰好一次。
func execLockKey(planID, trigger string) string {
	return "timertask/cron/exec/" + planID + "/" + trigger
}

// LocalMutex 是进程内 Mutex 实现（租约表 + TTL 惰性回收，零外部依赖）：
//
//   - 单实例部署的默认互斥（Scheduler 内建一份守触发 slot——摘旧装新的在飞
//     旧触发与新条目补跑可撞同一 slot，进程内先裁决，不劳分布式锁）；
//   - 多实例测试的共享假件：两个 Scheduler 共享同一个 LocalMutex 即可复现
//     跨实例仲裁（见 cron_test 的互斥并发场景）。
//
// 多实例生产部署请注入分布式实现（redisx/distlock 等，经 Mutex 缝）。
type LocalMutex struct {
	clock  local.Clock
	mu     sync.Mutex
	leases map[string]localLease
	seq    atomic.Uint64 // ownerID 生成（实例内单调，够用：ownerID 只需唯一性）
}

// localLease 是一张租约：持有者 + 过期时刻。
type localLease struct {
	owner   string
	expires time.Time
}

// NewLocalMutex 创建进程内互斥。clock 为租约到期判定的时钟，nil 取
// local.SystemClock{}（测试注入 FakeClock 可确定性推进 TTL）。
func NewLocalMutex(clock local.Clock) *LocalMutex {
	if clock == nil {
		clock = local.SystemClock{}
	}
	return &LocalMutex{clock: clock, leases: make(map[string]localLease)}
}

// TryLock 实现 Mutex：同键已有未过期租约 ⇒ ErrLockNotAcquired；过期租约
// 惰性回收（TTL 语义：崩溃持有者不尸占，到期即接管）。顺手清扫全部过期
// 租约——执行互斥按「认领到 TTL 到期」使用（见 Mutex 的 slot 认领注释），
// 同键之外的旧租约不会再被触碰，不扫会无界增长；规模 = 触发率 × TTL，
// 遍历开销可忽略。
func (m *LocalMutex) TryLock(_ context.Context, key string, ttl time.Duration) (string, error) {
	now := m.clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, l := range m.leases {
		if !l.expires.After(now) {
			delete(m.leases, k)
		}
	}
	if _, ok := m.leases[key]; ok {
		return "", ErrLockNotAcquired
	}
	owner := "local-" + strconv.FormatUint(m.seq.Add(1), 10)
	m.leases[key] = localLease{owner: owner, expires: now.Add(ttl)}
	return owner, nil
}

// Unlock 实现 Mutex：ownerID 匹配才删除；不存在/已易主返回错误（与
// distlock.ErrLockNotHeld 同义——调用方不得继续假装持有）。
func (m *LocalMutex) Unlock(_ context.Context, key string, ownerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[key]
	if !ok {
		return fmt.Errorf("cron: unlock %q: lock not held", key)
	}
	if l.owner != ownerID {
		return fmt.Errorf("cron: unlock %q: lock held by %q, not %q", key, l.owner, ownerID)
	}
	delete(m.leases, key)
	return nil
}
