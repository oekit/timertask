package reconcile

import (
	"strings"
	"testing"
)

// TestValidateDesiredSet_OK 无歧义期望集放行。
func TestValidateDesiredSet_OK(t *testing.T) {
	if err := ValidateDesiredSet("local", "key", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("合法期望集不应报错, got %v", err)
	}
	if err := ValidateDesiredSet("cron", "plan ID", nil); err != nil {
		t.Fatalf("空期望集不应报错, got %v", err)
	}
}

// TestValidateDesiredSet_EmptyID 空 ID fail-closed（错误文案与两层既有
// 输出逐字节一致——cron/local 的历史测试无感）。
func TestValidateDesiredSet_EmptyID(t *testing.T) {
	err := ValidateDesiredSet("local", "key", []string{"a", ""})
	if err == nil {
		t.Fatal("空 key 应报错")
	}
	if got, want := err.Error(), "local: reconcile: empty key in desired set"; got != want {
		t.Fatalf("err = %q, want %q", got, want)
	}
	err = ValidateDesiredSet("cron", "plan ID", []string{""})
	if err == nil {
		t.Fatal("空 plan ID 应报错")
	}
	if got, want := err.Error(), "cron: reconcile: empty plan ID in desired set"; got != want {
		t.Fatalf("err = %q, want %q", got, want)
	}
}

// TestValidateDesiredSet_DuplicateID 重复 ID fail-closed，报错携带重复值。
func TestValidateDesiredSet_DuplicateID(t *testing.T) {
	err := ValidateDesiredSet("cron", "plan ID", []string{"t1", "t2", "t1"})
	if err == nil {
		t.Fatal("重复 plan ID 应报错")
	}
	if got, want := err.Error(), `cron: reconcile: duplicate plan ID "t1" in desired set`; got != want {
		t.Fatalf("err = %q, want %q", got, want)
	}
	err = ValidateDesiredSet("local", "key", []string{"k1", "k1"})
	if err == nil || !strings.Contains(err.Error(), `duplicate key "k1"`) {
		t.Fatalf("重复 key 应报错并携带值, got %v", err)
	}
}
