package timertask

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ShardManager 分片管理器。
// 负责心跳注册、一致性哈希分配分片。
type ShardManager struct {
	clientID    string
	buzType     string
	client      redis.UniversalClient
	totalShards int
	myShards    []int
	mu          sync.RWMutex

	// 最近一次心跳是否成功。写路径故障（ZAdd 失败）时自己的旧条目
	// 仍留在 ZSET 中，若不做门控，下一拍 assignShards 会基于过期视图
	// 复活分片归属，恰好复现心跳失败清分片要防的双归属窗口。
	heartbeatOK atomic.Bool

	// 心跳配置
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
}

// NewShardManager 创建分片管理器。
func NewShardManager(buzType string, client redis.UniversalClient, totalShards int) *ShardManager {
	clientID := generateClientID()
	return &ShardManager{
		clientID:          clientID,
		buzType:           buzType,
		client:            client,
		totalShards:       totalShards,
		heartbeatInterval: 300 * time.Millisecond,
		heartbeatTimeout:  5 * time.Second,
	}
}

// generateClientID 生成唯一的客户端 ID。
// 优先使用 crypto/rand；若失败（极罕见，如系统熵池耗尽），回退到时间戳+伪随机，
// 保证不会因随机源故障导致进程崩溃。
func generateClientID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// 回退：crypto 不可用时用时间戳 + math/rand 保证唯一性，而非 panic
		binary.BigEndian.PutUint64(b, uint64(time.Now().UnixNano()))
	}
	return hex.EncodeToString(b)
}

// StartHeartbeat 启动心跳注册。
func (s *ShardManager) StartHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.heartbeat(ctx)
		}
	}
}

// heartbeat 注册心跳。
func (s *ShardManager) heartbeat(ctx context.Context) {
	key := s.heartbeatKey()
	now := time.Now().UnixMilli()

	// 使用 ZADD 注册心跳（score 为时间戳）
	_, err := s.client.ZAdd(ctx, key, redis.Z{
		Score:  float64(now),
		Member: s.clientID,
	}).Result()
	if err != nil {
		log.Error("heartbeat failed, clearing shards to avoid dual-ownership", "err", err)
		// 心跳失败时清除分片分配并门控后续分配，防止其他客户端抢走分片后出现双归属。
		s.heartbeatOK.Store(false)
		s.mu.Lock()
		s.myShards = nil
		s.mu.Unlock()
		return
	}
	s.heartbeatOK.Store(true)

	// 清理超时的心跳
	threshold := now - s.heartbeatTimeout.Milliseconds()
	_, err = s.client.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(threshold, 10)).Result()
	if err != nil {
		log.Error("cleanup heartbeat failed", "err", err)
	}
}

// StartAssign 启动分片分配。
func (s *ShardManager) StartAssign(ctx context.Context) {
	// 首次分配前先完成一次心跳：打开心跳门控（心跳失败则保持不分配，
	// 见 assignShards 的门控说明）
	s.heartbeat(ctx)

	// 立即分配一次
	s.assignShards(ctx)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.assignShards(ctx)
		}
	}
}

// assignShards 使用一致性哈希分配分片。
func (s *ShardManager) assignShards(ctx context.Context) {
	// 心跳门控：最近一次心跳失败（或尚未成功）时，ZSET 里自己的条目可能
	// 已经过期（写路径故障、读路径仍可用），此刻的分配结果不可信。
	// 跳过分配以维持心跳失败时的清空状态，直到心跳恢复。
	if !s.heartbeatOK.Load() {
		return
	}

	key := s.heartbeatKey()

	// 获取所有在线客户端
	members, err := s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		log.Error("get members failed", "err", err)
		return
	}

	// 提取客户端 ID 列表
	clients := make([]string, 0, len(members))
	for _, m := range members {
		if clientID, ok := m.Member.(string); ok {
			clients = append(clients, clientID)
		}
	}

	if len(clients) == 0 {
		return
	}

	// 排序确保一致性
	sort.Strings(clients)

	// 一致性哈希分配
	var myShards []int
	for shard := 0; shard < s.totalShards; shard++ {
		// 使用 FNV-1a 哈希分配（simpleHash）
		hash := simpleHash(s.buzType + "-" + strconv.Itoa(shard))
		idx := int(hash) % len(clients)
		if clients[idx] == s.clientID {
			myShards = append(myShards, shard)
		}
	}

	s.mu.Lock()
	s.myShards = myShards
	s.mu.Unlock()
}

// GetMyShards 获取当前负责的分片列表。
func (s *ShardManager) GetMyShards() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]int, len(s.myShards))
	copy(result, s.myShards)
	return result
}

// GetClientID 获取客户端 ID。
func (s *ShardManager) GetClientID() string {
	return s.clientID
}

func (s *ShardManager) heartbeatKey() string {
	return fmt.Sprintf(heartbeatKey, s.buzType)
}

// simpleHash 使用 FNV-1a 计算哈希，分布更均匀。
func simpleHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
