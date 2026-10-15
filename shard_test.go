package timertask

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestShardManager(t *testing.T, buzType string) (*ShardManager, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return NewShardManager(buzType, c, 16), mr
}

func TestNewShardManager_GeneratesUniqueID(t *testing.T) {
	a := NewShardManager("a", nil, 1)
	b := NewShardManager("a", nil, 1)
	if a.GetClientID() == b.GetClientID() {
		t.Errorf("expected unique client IDs, got %q twice", a.GetClientID())
	}
	if a.GetClientID() == "" {
		t.Error("clientID should be non-empty")
	}
}

func TestShardManager_GenerateClientID_Fallback(t *testing.T) {
	// generateClientID is best-effort: it always returns a hex string.
	// We can't easily force crypto/rand to fail in a portable way, so
	// we just verify the happy path produces 16 hex chars.
	id := generateClientID()
	if len(id) != 16 {
		t.Errorf("expected 16-char hex string, got %d chars: %q", len(id), id)
	}
}

func TestShardManager_StartHeartbeat_RunsThenStops(t *testing.T) {
	s, mr := newTestShardManager(t, "test-svc")
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.StartHeartbeat(ctx)
		close(done)
	}()

	// Wait long enough for at least one heartbeat tick (default 300ms).
	time.Sleep(400 * time.Millisecond)

	// Verify ZADD was applied to miniredis.
	members, _ := mr.ZMembers(s.heartbeatKey())
	if len(members) == 0 {
		t.Errorf("expected at least one heartbeat member, got 0")
	}
	cid := s.GetClientID()
	found := false
	for _, m := range members {
		if m == cid {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected clientID %q in heartbeat set", cid)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StartHeartbeat did not return after ctx cancel")
	}
}

func TestShardManager_StartAssign_AssignsShards(t *testing.T) {
	// 分配以心跳成功为前提（W1-T5 门控）：先完成一次心跳注册本客户端，
	// 再 run assignShards 确保 membership 非空时产生分片分配。
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = c.Close() }()
	s := NewShardManager("test-svc", c, 16)
	ctx := context.Background()

	s.heartbeat(ctx)
	s.assignShards(ctx)
	shards := s.GetMyShards()
	if len(shards) == 0 {
		t.Error("expected at least one assigned shard")
	}
	for _, sh := range shards {
		if sh < 0 || sh >= 16 {
			t.Errorf("shard %d out of range [0, 16)", sh)
		}
	}
}

func TestShardManager_StartAssign_EmptyMembership(t *testing.T) {
	s, _ := newTestShardManager(t, "test-svc")
	s.assignShards(context.Background())
	if got := s.GetMyShards(); len(got) != 0 {
		t.Errorf("expected 0 shards for empty membership, got %d", len(got))
	}
}

func TestShardManager_StartAssign_RunsThenStops(t *testing.T) {
	s, _ := newTestShardManager(t, "test-svc")
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.StartAssign(ctx)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond) // initial assign
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StartAssign did not return after ctx cancel")
	}
}

func TestShardManager_HeartbeatKey(t *testing.T) {
	s := NewShardManager("my-type", nil, 1)
	if got := s.heartbeatKey(); got != "{timertask.my-type}.heartbeat" {
		t.Errorf("heartbeatKey = %q, want {timertask.my-type}.heartbeat", got)
	}
}

func TestSimpleHash_DeterministicAndDiverse(t *testing.T) {
	a := simpleHash("hello")
	b := simpleHash("hello")
	if a != b {
		t.Errorf("simpleHash not deterministic: %d != %d", a, b)
	}
	if simpleHash("hello") == simpleHash("world") {
		t.Error("simpleHash should differentiate distinct inputs")
	}
}
