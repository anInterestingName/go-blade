// Package health 提供存活探针、就绪探针及 gRPC 健康检查服务的单元测试。
package health

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestHealth_HTTPProbes_LiveAndReady(t *testing.T) {
	var dbHealthy atomic.Bool
	dbHealthy.Store(true)

	checker := NewChecker(
		PingChecker("mysql", func(ctx context.Context) error {
			if !dbHealthy.Load() {
				return errors.New("connection refused")
			}
			return nil
		}),
		PingChecker("redis", func(ctx context.Context) error {
			return nil
		}),
	)

	// 1. LiveHandler 始终返回 HTTP 200 与 status: "UP"
	liveReq := httptest.NewRequest(http.MethodGet, "/healthz/live", nil)
	liveRec := httptest.NewRecorder()
	checker.LiveHandler()(liveRec, liveReq)
	require.Equal(t, http.StatusOK, liveRec.Code)

	var liveResp struct {
		Code    int          `json:"code"`
		Success bool         `json:"success"`
		Data    StatusReport `json:"data"`
	}
	require.NoError(t, json.Unmarshal(liveRec.Body.Bytes(), &liveResp))
	assert.Equal(t, 200, liveResp.Code)
	assert.True(t, liveResp.Success)
	assert.Equal(t, StatusUp, liveResp.Data.Status)

	// 2. ReadyHandler 在所有依赖正常时返回 HTTP 200 与各组件 UP
	readyReq1 := httptest.NewRequest(http.MethodGet, "/healthz/ready", nil)
	readyRec1 := httptest.NewRecorder()
	checker.ReadyHandler()(readyRec1, readyReq1)
	require.Equal(t, http.StatusOK, readyRec1.Code)

	var readyResp1 struct {
		Code    int          `json:"code"`
		Success bool         `json:"success"`
		Data    StatusReport `json:"data"`
	}
	require.NoError(t, json.Unmarshal(readyRec1.Body.Bytes(), &readyResp1))
	assert.Equal(t, 200, readyResp1.Code)
	assert.True(t, readyResp1.Success)
	assert.Equal(t, StatusUp, readyResp1.Data.Status)
	assert.Equal(t, StatusUp, readyResp1.Data.Components["mysql"])
	assert.Equal(t, StatusUp, readyResp1.Data.Components["redis"])

	// 3. 模拟 MySQL 故障降级：ReadyHandler 返回 HTTP 503 ServiceUnavailable，但 LiveHandler 依然保持 HTTP 200
	dbHealthy.Store(false)

	readyReq2 := httptest.NewRequest(http.MethodGet, "/healthz/ready", nil)
	readyRec2 := httptest.NewRecorder()
	checker.ReadyHandler()(readyRec2, readyReq2)
	require.Equal(t, http.StatusServiceUnavailable, readyRec2.Code)

	var readyResp2 struct {
		Code    int          `json:"code"`
		Success bool         `json:"success"`
		Msg     string       `json:"msg"`
		Data    StatusReport `json:"data"`
	}
	require.NoError(t, json.Unmarshal(readyRec2.Body.Bytes(), &readyResp2))
	assert.Equal(t, http.StatusServiceUnavailable, readyResp2.Code)
	assert.False(t, readyResp2.Success)
	assert.Equal(t, StatusDown, readyResp2.Data.Status)
	assert.Contains(t, readyResp2.Data.Components["mysql"], "connection refused")
	assert.Equal(t, StatusUp, readyResp2.Data.Components["redis"])

	liveRec2 := httptest.NewRecorder()
	LiveHandler()(liveRec2, httptest.NewRequest(http.MethodGet, "/healthz/live", nil))
	assert.Equal(t, http.StatusOK, liveRec2.Code)
}

func TestHealth_GRPCHealthServer(t *testing.T) {
	var serviceReady atomic.Bool
	serviceReady.Store(true)

	checker := NewManager()
	checker.Register("database", func(ctx context.Context) error {
		if !serviceReady.Load() {
			return errors.New("db down")
		}
		return nil
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	RegisterGRPCHealthServer(grpcSrv, checker)
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	defer grpcSrv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	healthClient := grpc_health_v1.NewHealthClient(conn)

	// 1. 依赖正常时返回 SERVING
	resp1, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{Service: "service-system"})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp1.GetStatus())

	// 2. 依赖故障时返回 NOT_SERVING
	serviceReady.Store(false)
	resp2, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{Service: "service-system"})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, resp2.GetStatus())
}
