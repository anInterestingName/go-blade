package sse

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-blade/pkg/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHub_TenantIsolationAndUserTargeting(t *testing.T) {
	hub := NewHub()

	c1, unreg1 := hub.Register("000000", 1001)
	defer unreg1()
	c2, unreg2 := hub.Register("000000", 1002)
	defer unreg2()
	cOther, unregOther := hub.Register("100001", 2001)
	defer unregOther()

	assert.Equal(t, 3, hub.ClientCount())

	// 1. 按租户 "000000" 广播：c1 和 c2 收到，cOther (租户 100001) 不会收到
	count := hub.BroadcastToTenant("000000", Event{
		Event: "notice.published",
		Data:  map[string]string{"title": "系统维护"},
	})
	assert.Equal(t, 2, count)

	ev1 := <-c1.Events
	ev2 := <-c2.Events
	assert.Equal(t, "notice.published", ev1.Event)
	assert.Equal(t, "notice.published", ev2.Event)
	assert.Len(t, cOther.Events, 0)

	// 2. 按用户 1002 定向单播
	uCount := hub.SendToUser("000000", 1002, Event{
		Event: "private.msg",
		Data:  "hello 1002",
	})
	assert.Equal(t, 1, uCount)
	assert.Len(t, c1.Events, 0)
	evPrivate := <-c2.Events
	assert.Equal(t, "private.msg", evPrivate.Event)
}

func TestStreamHandler_HTTPStreaming(t *testing.T) {
	hub := NewHub()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := tenant.WithTenant(r.Context(), "888000")
		hub.StreamHandler()(w, r.WithContext(ctx))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/stream", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Contains(t, resp.Header.Get("Content-Type"), "text/event-stream")

	reader := bufio.NewReader(resp.Body)
	// 读取首个 connected 事件
	line1, err := reader.ReadString('\n')
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(line1, "id: "))

	// 异步向租户 888000 广播一条公告事件
	go func() {
		time.Sleep(25 * time.Millisecond)
		hub.BroadcastToTenant("888000", Event{
			ID:    "evt-99",
			Event: "notice.published",
			Data:  map[string]string{"title": "实时公告推送"},
		})
	}()

	var collected strings.Builder
	for i := 0; i < 6; i++ {
		line, rErr := reader.ReadString('\n')
		if rErr != nil {
			break
		}
		collected.WriteString(line)
		if strings.Contains(collected.String(), "实时公告推送") {
			break
		}
	}
	assert.Contains(t, collected.String(), "event: notice.published")
	assert.Contains(t, collected.String(), "实时公告推送")
}
