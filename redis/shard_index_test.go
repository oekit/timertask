package redis

import (
	"bytes"
	"os"
	"testing"
)

// TestShardIndex_NeverNegative 回归测试：分片下标恒在 [0, n)。
//
// 缺陷成因：原实现是 `int(hash) % len(clients)`，hash 是 uint32。32 位平台上
// int 为 32 位有符号数，hash > MaxInt32 时转换按位截断成**负数**（约一半
// 概率）⇒ 负数 % n 在 Go 里仍是负数 ⇒ clients[idx] 越界 panic ⇒ 分片分配
// goroutine 当场死亡。
//
// 本测试用 int32 显式模拟 32 位平台的截断，证明旧算法会给出负下标、新算法
// 不会。
func TestShardIndex_NeverNegative(t *testing.T) {
	const n = 7
	for _, h := range []uint32{0, 1, 0x7FFFFFFF, 0x80000000, 0xC0000000, 0xFFFFFFFF} {
		got := shardIndex(h, n)
		if got < 0 || got >= n {
			t.Fatalf("shardIndex(%#x, %d) = %d，越界", h, n, got)
		}
	}
	// 穷举一段高位区间：全部都必须落在 [0, n)
	for h := uint32(0x80000000); h < 0x80000000+10000; h++ {
		got := shardIndex(h, n)
		if got < 0 || got >= n {
			t.Fatalf("shardIndex(%#x, %d) = %d，越界", h, n, got)
		}
	}
}

// TestShardIndex_LegacyExpressionGoesNegative 钉住缺陷本身：模拟 32 位平台的
// `int(hash) % n` 确实会给出负下标（这正是原实现的写法）。
//
// 没有这条断言，上面的测试会退化成「新函数自己对自己」的空转检查。
func TestShardIndex_LegacyExpressionGoesNegative(t *testing.T) {
	const n = 7
	found := false
	for _, h := range []uint32{0x80000000, 0xC0000000, 0xFFFFFFFF} {
		legacy := int(int32(h)) % n // 32 位平台上 int(hash) 的等价行为
		if legacy < 0 {
			found = true
			// 新算法对同一个哈希值必须给出合法下标
			got := shardIndex(h, n)
			if got < 0 || got >= n {
				t.Fatalf("shardIndex(%#x, %d) = %d，越界", h, n, got)
			}
		}
	}
	if !found {
		t.Fatal("样本哈希值未复现 32 位截断为负的现象（测试失去意义）")
	}
}

// TestShardIndex_ZeroClients 护栏：客户端列表为空时不得除零/越界。
func TestShardIndex_ZeroClients(t *testing.T) {
	if got := shardIndex(12345, 0); got != 0 {
		t.Fatalf("shardIndex(_, 0) = %d，期望 0", got)
	}
}

// TestShardIndex_NoIntTruncationInSource 源码级护栏：分片下标计算不得再出现
// 「把 uint32 强转成 int 再取模」这类写法。
//
// 为什么不靠行为测试兜住：uint32 → int 的截断**只在 32 位平台上**产生负数
// （64 位平台上 int 是 64 位 ⇒ 强转结果恒为正）⇒ 任何黑盒测试在 64 位 CI
// 上都测不出这个差异 ⇒ 只能从源码形态上钉住。
func TestShardIndex_NoIntTruncationInSource(t *testing.T) {
	src, err := os.ReadFile("shard.go")
	if err != nil {
		t.Fatalf("read shard.go: %v", err)
	}
	// 只看代码行（跳过注释）：注释里可以解释这个坑。
	for i, line := range bytes.Split(src, []byte("\n")) {
		trimmed := bytes.TrimSpace(line)
		if bytes.HasPrefix(trimmed, []byte("//")) {
			continue
		}
		if bytes.Contains(trimmed, []byte("int(hash)")) {
			t.Fatalf("shard.go:%d 出现 `int(hash)`：32 位平台上 uint32→int 截断成负数 ⇒ 负下标越界 panic", i+1)
		}
	}
}
