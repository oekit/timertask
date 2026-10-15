package timertask

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestNewRedisBackend_SetsDefaults(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = c.Close() }()

	b := NewRedisBackend("test", c, func(ctx context.Context, t *Task) error { return nil })
	if b == nil {
		t.Fatal("NewRedisBackend returned nil")
	}
	if b.buzType != "test" {
		t.Errorf("buzType = %q, want test", b.buzType)
	}
	if b.shardManager == nil {
		t.Error("shardManager should be initialized")
	}
	if b.shardManager.buzType != "test" {
		t.Errorf("shardManager.buzType = %q, want test", b.shardManager.buzType)
	}
	if cap(b.localQueue) != 100000 {
		t.Errorf("localQueue cap = %d, want 100000", cap(b.localQueue))
	}
}

func TestNewRedisBackend_AppliesOptions(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = c.Close() }()

	b := NewRedisBackend("test", c, func(ctx context.Context, t *Task) error { return nil },
		WithBuckets(8),
		WithWorkers(4),
		WithRetry(3, 50*time.Millisecond),
	)
	if b.buckets != 8 {
		t.Errorf("buckets = %d, want 8", b.buckets)
	}
	if b.workers != 4 {
		t.Errorf("workers = %d, want 4", b.workers)
	}
	if b.retryCount != 3 {
		t.Errorf("retryCount = %d, want 3", b.retryCount)
	}
	if b.retryDelay != 50*time.Millisecond {
		t.Errorf("retryDelay = %v, want 50ms", b.retryDelay)
	}
}

func TestRedisBackend_Start_RunsThenStops(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = c.Close() }()

	b := NewRedisBackend("test", c, func(ctx context.Context, t *Task) error { return nil },
		WithWorkers(1),
	)
	ctx, cancel := context.WithCancel(context.Background())

	startErr := make(chan error, 1)
	go func() { startErr <- b.Start(ctx) }()

	// Let the goroutines spin up.
	time.Sleep(100 * time.Millisecond)

	cancel()

	select {
	case err := <-startErr:
		if err != context.Canceled {
			t.Errorf("Start returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return within 2s after cancel")
	}
}
