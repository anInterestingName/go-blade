package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_PhaseOrder_LIFO_And_PanicIsolation(t *testing.T) {
	mgr := NewManager(2 * time.Second)

	var mu sync.Mutex
	var executionOrder []string
	record := func(name string) {
		mu.Lock()
		executionOrder = append(executionOrder, name)
		mu.Unlock()
	}

	// 故意乱序注册不同 Phase 的停机钩子，并在同 Phase 内注册多个钩子验证 LIFO 逆序
	mgr.Register("tracer-flush", PhaseObservability, func(ctx context.Context) error {
		record("5-tracer-flush")
		return nil
	})
	mgr.Register("mq-broker-close", PhaseMessageQueue, func(ctx context.Context) error {
		record("4-mq-broker-close")
		return nil
	})
	mgr.Register("job-scheduler-stop", PhaseBackgroundWorker, func(ctx context.Context) error {
		record("3-job-scheduler-stop")
		panic("simulated panic in background worker shutdown")
	})
	mgr.Register("http-server-stop", PhaseServerStop, func(ctx context.Context) error {
		record("2b-http-server-stop")
		return nil
	})
	mgr.Register("grpc-server-stop", PhaseServerStop, func(ctx context.Context) error {
		record("2a-grpc-server-stop")
		return nil
	})
	mgr.Register("nacos-deregister", PhaseTrafficDrain, func(ctx context.Context) error {
		record("1-nacos-deregister")
		return nil
	})

	err := mgr.Shutdown(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panicked")

	expected := []string{
		"1-nacos-deregister",
		"2a-grpc-server-stop",
		"2b-http-server-stop",
		"3-job-scheduler-stop",
		"4-mq-broker-close",
		"5-tracer-flush",
	}
	assert.Equal(t, expected, executionOrder)
}

func TestManager_WaitForSignal_ContextCancel(t *testing.T) {
	mgr := NewManager(time.Second)
	called := false
	mgr.OnShutdown("cleanup", func(ctx context.Context) error {
		called = true
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	require.NoError(t, mgr.WaitForSignal(ctx))
	assert.True(t, called)
}
