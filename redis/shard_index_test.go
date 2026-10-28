package redis

import (
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// hashTokens 是「这是个哈希值」的标记：uint32 的来源。
var hashTokens = []string{"crc32", "fnv", "Sum32", "Checksum", "hash", "Hash"}

// TestShardIndex_NoIntTruncationInSource 源码级护栏：**任何文件**里都不得再出现
// 「把 uint32 强转成 int 再取模」这类写法。
//
// 为什么不靠行为测试兜住：uint32 → int 的截断**只在 32 位平台上**产生负数
// （64 位平台上 int 是 64 位 ⇒ 强转结果恒为正）⇒ 任何黑盒测试在 64 位 CI
// 上都测不出这个差异 ⇒ 只能从源码形态上钉住。
//
// **为什么扫全目录而不是只扫 shard.go**（#42）：旧护栏只读 `shard.go`，
// 而同包的 `redis.go` 里有一处**完全同型**的写法
// （`int(crc32.ChecksumIEEE(...)) % b.buckets`）⇒ 旧护栏对它**完全失明**。
// ⇒ 「同型缺陷在别的文件里再出现一次」这件事，没有任何机制会报。
//
// **为什么不用字面量匹配**（#42）：旧护栏匹配的是字面量 `int(hash)`，
// 而真实写法是 `int(crc32.ChecksumIEEE([]byte(taskID)))` ⇒ 即使把扫描范围
// 扩到全目录，字面量也**匹配不到**。现在改为**形态匹配**：找「把哈希值转成
// int 的强制转换」且该转换**后面紧跟取模**的行。
func TestShardIndex_NoIntTruncationInSource(t *testing.T) {
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("未找到任何 .go 文件，扫描范围可能不对")
	}

	for _, path := range entries {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue // 注释里可以解释这个坑
			}
			if castsHashToInt(trimmed) && hasModuloOutsideCast(trimmed) {
				t.Fatalf("%s:%d 出现「uint32 哈希值强转 int 后再取模」：32 位平台上转换按位截断成负数 "+
					"⇒ 负下标/负分片。修法：先在 uint32 空间取模（shardIndex）", path, i+1)
			}
		}
	}
}

// castsHashToInt 判定该行是否含「把哈希值强转成 int」的强制转换。
func castsHashToInt(line string) bool {
	idx := strings.Index(line, "int(")
	if idx < 0 {
		return false
	}
	inner := line[idx+len("int("):]
	for _, tok := range hashTokens {
		if strings.Contains(inner, tok) {
			return true
		}
	}
	return false
}

// hasModuloOutsideCast 判定「取模」出现在强制转换**之外**。
//
// 目标形态：`int(crc32.ChecksumIEEE(...)) % n`  ← 取模在转换之后（危险）
// 安全形态：`int(hash % uint32(n))`              ← 取模在转换之内（正确）
func hasModuloOutsideCast(line string) bool {
	idx := strings.Index(line, "int(")
	if idx < 0 {
		return false
	}
	rest := line[idx+len("int("):]
	depth := 1
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.HasPrefix(strings.TrimSpace(rest[i+1:]), "%")
			}
		}
	}
	return false
}

// TestTaskKey_ShardInRange 行为契约：taskKey 产出的分片段恒在 [0, buckets)。
//
// 这条在 64 位平台上**两种实现都通过**（64 位 int 恒正）⇒ 它不是缺陷的判据，
// 而是「分片下标必须合法」的契约护栏；真正的判据是上面的源码形态护栏
// （32 位差异无法用黑盒测出）。
func TestTaskKey_ShardInRange(t *testing.T) {
	const buckets = 16
	b := &RedisBackend{buckets: buckets, buzType: "order"}
	for i := 0; i < 5000; i++ {
		key := b.taskKey("task-" + strconv.Itoa(i))
		idx := strings.LastIndex(key, ".shard.")
		if idx < 0 {
			t.Fatalf("taskKey 形态异常: %q", key)
		}
		seg := key[idx+len(".shard."):]
		n, err := strconv.Atoi(seg)
		if err != nil {
			t.Fatalf("分片段不是整数: %q", seg)
		}
		if n < 0 || n >= buckets {
			t.Fatalf("分片段越界: %q（buckets=%d）", seg, buckets)
		}
	}
}

// TestTaskKey_LegacyExpressionGoesNegative 钉住缺陷本身：旧写法
// `int(crc32.ChecksumIEEE(...)) % buckets` 在 32 位平台上会给出负分片。
//
// 没有这条断言，护栏会退化成「新函数自己对自己」的空转检查。
func TestTaskKey_LegacyExpressionGoesNegative(t *testing.T) {
	const buckets = 16
	found := false
	for i := 0; i < 5000 && !found; i++ {
		h := crc32.ChecksumIEEE([]byte("task-" + strconv.Itoa(i)))
		if legacy := int(int32(h)) % buckets; legacy < 0 { // 32 位平台上 int(uint32) 的等价行为
			found = true
			// 新算法对同一个哈希值必须给出合法分片
			if got := shardIndex(h, buckets); got < 0 || got >= buckets {
				t.Fatalf("shardIndex(%#x, %d) = %d，越界", h, buckets, got)
			}
		}
	}
	if !found {
		t.Fatal("样本哈希值未复现 32 位截断为负的现象（测试失去意义）")
	}
}
