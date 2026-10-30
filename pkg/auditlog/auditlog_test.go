// Package auditlog 提供敏感字段脱敏与 API 操作审计日志中间件的单元测试。
package auditlog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-blade/pkg/event"
	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaskPhone_And_MaskJSON(t *testing.T) {
	assert.Equal(t, "138****5678", MaskPhone("13812345678"))
	assert.Equal(t, "139****0000", MaskPhone(" 13900000000 "))
	assert.Equal(t, "", MaskPhone(""))

	rawPayload := map[string]any{
		"account":       "admin",
		"password":      "PlainSecret123!",
		"old_password":  "OldPass456",
		"newPassword":   "NewPass789",
		"access_token":  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
		"refreshToken":  "rt-secret-token",
		"captcha_code":  "8421",
		"secret":        "top-secret-key",
		"phone":         "13812345678",
		"mobile":        "15987654321",
		"nested_object": map[string]any{"user_password": "stay_plain_or_mask", "password": "inner_password", "phone": "18611112222"},
	}
	rawBytes, err := json.Marshal(rawPayload)
	require.NoError(t, err)

	maskedStr := MaskJSON(rawBytes)
	var maskedMap map[string]any
	require.NoError(t, json.Unmarshal([]byte(maskedStr), &maskedMap))

	assert.Equal(t, "admin", maskedMap["account"])
	assert.Equal(t, MaskedPlaceholder, maskedMap["password"])
	assert.Equal(t, MaskedPlaceholder, maskedMap["old_password"])
	assert.Equal(t, MaskedPlaceholder, maskedMap["newPassword"])
	assert.Equal(t, MaskedPlaceholder, maskedMap["access_token"])
	assert.Equal(t, MaskedPlaceholder, maskedMap["refreshToken"])
	assert.Equal(t, MaskedPlaceholder, maskedMap["captcha_code"])
	assert.Equal(t, MaskedPlaceholder, maskedMap["secret"])
	assert.Equal(t, "138****5678", maskedMap["phone"])
	assert.Equal(t, "159****4321", maskedMap["mobile"])

	nested := maskedMap["nested_object"].(map[string]any)
	assert.Equal(t, MaskedPlaceholder, nested["password"])
	assert.Equal(t, "186****2222", nested["phone"])

	// 非 JSON 输入安全回退
	nonJSON := MaskJSON([]byte("plain text body"))
	assert.Equal(t, "plain text body", nonJSON)
}

func TestAuditMiddleware_Desensitization_And_BodyPreservation(t *testing.T) {
	tpHolder, err := trace.InitTracer(trace.Config{
		ServiceName: "test-audit-service",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	defer func() { _ = tpHolder.Shutdown(context.Background()) }()

	memRecorder := NewMemoryRecorder(100)
	bus := event.NewAsyncBus(event.WithWorkers(2), event.WithBufferSize(32))
	defer func() { _ = bus.Close(context.Background()) }()

	var eventReceived Entry
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	bus.Subscribe(AuditEventTopic, func(ctx context.Context, msg event.Message) error {
		defer eventWg.Done()
		if e, ok := msg.Payload.(Entry); ok {
			eventReceived = e
		}
		return nil
	})

	multiRec := NewMultiRecorder(memRecorder, NewEventRecorder(bus))
	auditMW := AuditMiddleware("service-gateway", multiRec)

	var downstreamDecoded map[string]any
	handler := auditMW(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewDecoder(r.Body).Decode(&downstreamDecoded)
		require.NoError(t, err)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"code":200}`)
	})

	reqBody, _ := json.Marshal(map[string]any{
		"account":  "new_user",
		"password": "MyRawPassword999",
		"phone":    "13812345678",
	})

	req := httptest.NewRequest(http.MethodPost, "/blade-system/user/submit?source=admin", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-For", "10.20.30.40")

	ctx := tenant.WithTenant(req.Context(), "100001")
	ctx = jwt.WithClaims(ctx, &jwt.BladeClaims{
		TenantID: "100001",
		UserID:   9007199254740993,
		Account:  "super_admin",
	})
	ctx, span := trace.StartSpan(ctx, "POST /blade-system/user/submit")
	expectedTraceID := trace.TraceIDFromContext(ctx)

	w := httptest.NewRecorder()
	handler(w, req.WithContext(ctx))
	span.End()

	// 1. 验证下游 Handler 读取到的依然是未经修改的原始密码与手机号
	assert.Equal(t, "MyRawPassword999", downstreamDecoded["password"])
	assert.Equal(t, "13812345678", downstreamDecoded["phone"])

	// 2. 验证 MemoryRecorder 中的审计记录已脱敏且元数据完整
	entries := memRecorder.Entries()
	require.Len(t, entries, 1)
	entry := entries[0]
	assert.Equal(t, "100001", entry.TenantID)
	assert.Equal(t, "service-gateway", entry.ServiceName)
	assert.Equal(t, int64(9007199254740993), entry.UserID.Int64())
	assert.Equal(t, "super_admin", entry.Account)
	assert.Equal(t, http.MethodPost, entry.Method)
	assert.Equal(t, "/blade-system/user/submit", entry.Path)
	assert.Equal(t, "source=admin", entry.Query)
	assert.Equal(t, http.StatusCreated, entry.StatusCode)
	assert.Equal(t, "10.20.30.40", entry.ClientIP)
	assert.Equal(t, expectedTraceID, entry.TraceID)
	assert.NotContains(t, entry.RequestBody, "MyRawPassword999")
	assert.Contains(t, entry.RequestBody, MaskedPlaceholder)
	assert.Contains(t, entry.RequestBody, "138****5678")

	// 3. 验证 EventRecorder 同步将审计记录投递到了事件总线
	done := make(chan struct{})
	go func() {
		eventWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for audit event from EventRecorder")
	}
	assert.Equal(t, entry.Path, eventReceived.Path)
	assert.Equal(t, entry.TraceID, eventReceived.TraceID)
}

func TestMaskIDCard_MaskString_And_RingBufferEviction(t *testing.T) {
	// 1. 身份证号脱敏
	assert.Equal(t, "", MaskIDCard("   "))
	assert.Equal(t, "110101********1234", MaskIDCard("110101199003071234"))
	assert.Equal(t, "110101********123X", MaskIDCard("110101900307123X"))
	assert.Equal(t, "1****5", MaskIDCard("12345"))
	assert.Equal(t, MaskedPlaceholder, MaskPhone("12"))
	assert.Equal(t, "1****4", MaskPhone("1234"))

	// 2. MaskString 按键名脱敏
	assert.Equal(t, MaskedPlaceholder, MaskString("Old_Password", "secret123"))
	assert.Equal(t, "138****5678", MaskString("contact_number", "13812345678"))
	assert.Equal(t, "110101********1234", MaskString("id_card", "110101199003071234"))
	assert.Equal(t, "normal_val", MaskString("username", "normal_val"))

	// 3. MaskJSON 边界：含尾随垃圾字符的非 JSON、超长 UTF-8 截断与数组嵌套
	nonJSONWithSpace := "123456 not a json payload"
	assert.Equal(t, nonJSONWithSpace, MaskJSON([]byte(nonJSONWithSpace)))

	longChinese := bytes.Repeat([]byte("中文测试"), 200) // 800 字符 > 512
	truncated := MaskJSON(longChinese)
	assert.True(t, utf8.ValidString(truncated), "截断后的多字节 UTF-8 字符串必须保持合法编码")
	assert.Contains(t, truncated, "...(truncated)")

	// 4. MemoryRecorder 环形容量上限淘汰验证
	ring := NewMemoryRecorder(3)
	for i := 1; i <= 5; i++ {
		require.NoError(t, ring.Record(context.Background(), Entry{
			Account: "user",
			Path:    string(rune('A' + i - 1)),
		}))
	}
	entries := ring.Entries()
	require.Len(t, entries, 3)
	assert.Equal(t, "C", entries[0].Path)
	assert.Equal(t, "D", entries[1].Path)
	assert.Equal(t, "E", entries[2].Path)
}

func FuzzMaskJSON(f *testing.F) {
	f.Add([]byte(`{"account":"admin","password":"123","phone":"13800138000","id_card":"110101199003071234"}`))
	f.Add([]byte(`[{"secret":"abc"},{"nested":{"refresh_token":"xyz"}}]`))
	f.Add([]byte(`12345 trailing garbage`))
	f.Add([]byte(`invalid json {`))
	f.Add([]byte(``))

	f.Fuzz(func(t *testing.T, raw []byte) {
		assert.NotPanics(t, func() {
			res := MaskJSON(raw)
			if utf8.Valid(raw) {
				assert.True(t, utf8.ValidString(res))
			}
		})
	})
}
