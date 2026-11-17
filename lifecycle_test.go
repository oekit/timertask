package timertask

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestDefaultShutdownDrainGrace_Parity 钉住 #262 的单源前提：Local/Redis
// 两后端的停机排水宽限此前各持一份 10s 常量，收敛后值必须仍是 10s——
// 两后端停机预算不得在收敛时被悄然改动。
func TestDefaultShutdownDrainGrace_Parity(t *testing.T) {
	if DefaultShutdownDrainGrace != 10*time.Second {
		t.Fatalf("DefaultShutdownDrainGrace = %v, want 10s（两后端原常量同值）", DefaultShutdownDrainGrace)
	}
}

// TestStartState_ThreeStates 钉住 #124 三态检查的映射单源（#262）：
// 未启动放行并登记 started；已启动返回 ErrBackendAlreadyStarted；
// 停机态返回 ErrBackendStopped（终态优先于 started——Start 返回后
// 二次 Start 报 stopped 而非 already started）。
func TestStartState_ThreeStates(t *testing.T) {
	var st StartState

	if err := st.BeginStartLocked(); err != nil {
		t.Fatalf("未启动 BeginStartLocked = %v, want nil", err)
	}
	if err := st.BeginStartLocked(); !errors.Is(err, ErrBackendAlreadyStarted) {
		t.Fatalf("已启动 BeginStartLocked = %v, want ErrBackendAlreadyStarted", err)
	}
	st.MarkStoppedLocked()
	if !st.StoppedLocked() {
		t.Fatal("MarkStoppedLocked 后 StoppedLocked 应为 true")
	}
	if err := st.BeginStartLocked(); !errors.Is(err, ErrBackendStopped) {
		t.Fatalf("停机态 BeginStartLocked = %v, want ErrBackendStopped", err)
	}
}

// TestDrainWorkers_Drains 排空路径：wg 全部 Done 后返回 true。
func TestDrainWorkers_Drains(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
		}()
	}
	start := time.Now()
	if !DrainWorkers(&wg, 5*time.Second) {
		t.Fatal("wg 排空应返回 true")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("排空后应立即返回, took %v", elapsed)
	}
}

// TestDrainWorkers_BoundedGrace 超窗路径：宽限到期即返回 false（有界等待），
// 不无限阻塞——waiter 仍在途时 grace=50ms 的等待应在毫秒级收口。
func TestDrainWorkers_BoundedGrace(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1) // 永不 Done：模拟 worker 卡在慢 handler
	start := time.Now()
	if DrainWorkers(&wg, 50*time.Millisecond) {
		t.Fatal("未排空应返回 false")
	}
	if elapsed := time.Since(start); elapsed >= 5*time.Second {
		t.Fatalf("宽限未生效: elapsed = %v", elapsed)
	}
	// 收尾：放掉 waiter，避免泄漏进其它测试。
	wg.Done()
}
