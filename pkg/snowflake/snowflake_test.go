// Package snowflake 提供具备防前端 JavaScript 精度溢出特性的雪花算法生成器与动态节点分配器的单元测试。
package snowflake

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"

	pkgredis "github.com/go-blade/pkg/redis"
)

// TestItem 测试雪花 ID 序列化与反序列化的示例结构体。
type TestItem struct {
	ID   ID     `json:"id"`
	Name string `json:"name"`
}

// TestSnowflake 验证雪花节点基础初始化与 JSON 字符串/数字双模序列化。
func TestSnowflake(t *testing.T) {
	// 测试非法 nodeID 报错，且不会影响后续正常初始化
	err := InitSnowflake(-1)
	if err == nil {
		t.Fatal("expected error with negative node ID, got nil")
	}

	err = InitSnowflake(1024)
	if err == nil {
		t.Fatal("expected error with out-of-range node ID, got nil")
	}

	// 即使之前非法初始化失败，合法初始化依然能正常生效
	err = InitSnowflake(100)
	if err != nil {
		t.Fatalf("init snowflake failed: %v", err)
	}

	id := NextID()
	if id.Int64() <= 0 {
		t.Fatalf("invalid generated id: %v", id)
	}

	item := TestItem{
		ID:   id,
		Name: "test-item",
	}

	bytes, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("json marshal failed: %v", err)
	}

	expectedStr := `"` + id.String() + `"`
	if !strings.Contains(string(bytes), expectedStr) {
		t.Fatalf("expected serialized JSON to contain string ID %s, got: %s", expectedStr, string(bytes))
	}

	var parsed TestItem
	if err := json.Unmarshal(bytes, &parsed); err != nil {
		t.Fatalf("json unmarshal failed: %v", err)
	}

	if parsed.ID != id {
		t.Fatalf("parsed ID %v does not match original %v", parsed.ID, id)
	}

	// 测试字符串与数字兼容反序列化
	var fromNum TestItem
	if err := json.Unmarshal([]byte(`{"id":123456789,"name":"num"}`), &fromNum); err != nil {
		t.Fatalf("unmarshal from number failed: %v", err)
	}
	if fromNum.ID.Int64() != 123456789 {
		t.Fatalf("expected 123456789, got %v", fromNum.ID.Int64())
	}

	var fromEmpty TestItem
	if err := json.Unmarshal([]byte(`{"id":"","name":"empty"}`), &fromEmpty); err != nil {
		t.Fatalf("unmarshal from empty string failed: %v", err)
	}
	if fromEmpty.ID != 0 {
		t.Fatalf("expected 0, got %v", fromEmpty.ID)
	}

	// 测试 GenIDString
	strID := GenIDString()
	if len(strID) == 0 {
		t.Fatal("expected non-empty string ID")
	}
}

// TestInitAutoNode_WithRedis 验证在 miniredis 可用时 InitAutoNode 连续分配递增不冲突的 NodeID 且支持 0~1023 循环回绕。
func TestInitAutoNode_WithRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := zeroredis.MustNewRedis(zeroredis.RedisConf{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	})
	pkgredis.SetRedis(client)
	defer pkgredis.SetRedis(nil)

	ctx := context.Background()

	// 连续分配前 5 个节点，应当依次返回 0, 1, 2, 3, 4
	for expected := int64(0); expected < 5; expected++ {
		nodeID, err := InitAutoNode(ctx, "service-auth")
		if err != nil {
			t.Fatalf("InitAutoNode failed at step %d: %v", expected, err)
		}
		if nodeID != expected {
			t.Fatalf("expected nodeID %d, got %d", expected, nodeID)
		}
		if GenID() <= 0 {
			t.Fatal("expected valid generated snowflake ID")
		}
	}

	// 模拟计数器达到 1023，下一次 INCR 为 1024 -> (1024-1)%1024 = 1023，再下一次 INCR 为 1025 -> (1025-1)%1024 = 0（循环回绕）
	_ = mr.Set(RedisNodeKeyPrefix+"service-auth", strconv.FormatInt(MaxNodeID, 10))

	node1023, err := InitAutoNode(ctx, "service-auth")
	if err != nil {
		t.Fatalf("InitAutoNode for 1023 failed: %v", err)
	}
	if node1023 != MaxNodeID {
		t.Fatalf("expected boundary nodeID %d, got %d", MaxNodeID, node1023)
	}

	nodeWrap, err := InitAutoNode(ctx, "service-auth")
	if err != nil {
		t.Fatalf("InitAutoNode wrap-around failed: %v", err)
	}
	if nodeWrap != 0 {
		t.Fatalf("expected wrapped nodeID 0, got %d", nodeWrap)
	}
}

// TestInitAutoNode_FallbackWithoutRedis 验证在未初始化 Redis 或 Redis 故障时自动回退至本机指纹哈希计算确定性 NodeID。
func TestInitAutoNode_FallbackWithoutRedis(t *testing.T) {
	pkgredis.SetRedis(nil)

	ctx := context.Background()
	nodeID1, err := InitAutoNode(ctx, "service-system")
	if err != nil {
		t.Fatalf("InitAutoNode fallback failed: %v", err)
	}
	if nodeID1 < 0 || nodeID1 > MaxNodeID {
		t.Fatalf("fallback nodeID %d out of range [0, %d]", nodeID1, MaxNodeID)
	}

	// 同一进程与相同服务名应产生确定性的相同指纹 NodeID
	nodeID2, err := InitAutoNode(ctx, "service-system")
	if err != nil {
		t.Fatalf("second InitAutoNode fallback failed: %v", err)
	}
	if nodeID1 != nodeID2 {
		t.Fatalf("expected deterministic fallback nodeID %d, got %d", nodeID1, nodeID2)
	}

	if GenID() <= 0 {
		t.Fatal("expected valid snowflake ID after fallback init")
	}
}
