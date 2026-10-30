package response

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/go-blade/pkg/xerr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResponse(t *testing.T) {
	ctx := context.Background()

	// 1. 测试成功响应包装
	w := httptest.NewRecorder()
	ResultResponse(ctx, w, "hello blade", nil)
	var resp Response[string]
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Code != 200 || !resp.Success || resp.Data != "hello blade" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	// 2. 测试业务错误 CodeError
	wErr := httptest.NewRecorder()
	ResultResponse(ctx, wErr, nil, xerr.NewCodeError(xerr.TokenExpired, "token has expired"))
	var errResp Response[any]
	if err := json.Unmarshal(wErr.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if errResp.Code != xerr.TokenExpired || errResp.Success || errResp.Msg != "token has expired" {
		t.Fatalf("unexpected error response: %+v", errResp)
	}

	// 3. 测试未知系统错误：不应泄露底层 sql/path 等敏感错误字符串
	wUnknown := httptest.NewRecorder()
	ResultResponse(ctx, wUnknown, nil, errors.New("sql: access denied for user root@localhost (using password: YES)"))
	var unknownResp Response[any]
	if err := json.Unmarshal(wUnknown.Body.Bytes(), &unknownResp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if unknownResp.Code != xerr.ServerCommonError || unknownResp.Success {
		t.Fatalf("unexpected code for unknown error: %+v", unknownResp)
	}
	if unknownResp.Msg != xerr.MapErrMsg(xerr.ServerCommonError) {
		t.Fatalf("internal error leaked! msg: %s", unknownResp.Msg)
	}

	// 4. 测试 gRPC 错误映射
	wGrpc := httptest.NewRecorder()
	grpcErr := status.Error(codes.NotFound, "user entity not found")
	ResultResponse(ctx, wGrpc, nil, grpcErr)
	var grpcResp Response[any]
	if err := json.Unmarshal(wGrpc.Body.Bytes(), &grpcResp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if grpcResp.Code != xerr.NotFound || grpcResp.Msg != "user entity not found" {
		t.Fatalf("unexpected grpc mapped response: %+v", grpcResp)
	}

	// 5. 测试 gRPC 内部异常强制脱敏：禁止暴露 codes.Internal / codes.Unknown 中的原始报错或 panic 信息
	wGrpcInternal := httptest.NewRecorder()
	grpcInternalErr := status.Error(codes.Internal, "internal server error: panic runtime error: index out of range [0]")
	ResultResponse(ctx, wGrpcInternal, nil, grpcInternalErr)
	var grpcInternalResp Response[any]
	if err := json.Unmarshal(wGrpcInternal.Body.Bytes(), &grpcInternalResp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if grpcInternalResp.Code != xerr.ServerCommonError || grpcInternalResp.Msg != xerr.MapErrMsg(xerr.ServerCommonError) {
		t.Fatalf("grpc internal error was not sanitized: %+v", grpcInternalResp)
	}

	// 6. 测试业务错误经 gRPC 编码后无损还原
	wGrpcBiz := httptest.NewRecorder()
	bizRpcErr := xerr.ToGrpcError(xerr.NewCodeError(xerr.CaptchaError, "图形验证码不匹配"))
	ResultResponse(ctx, wGrpcBiz, nil, bizRpcErr)
	var grpcBizResp Response[any]
	if err := json.Unmarshal(wGrpcBiz.Body.Bytes(), &grpcBizResp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if grpcBizResp.Code != xerr.CaptchaError || grpcBizResp.Msg != "图形验证码不匹配" {
		t.Fatalf("expected restored biz code %d, got: %+v", xerr.CaptchaError, grpcBizResp)
	}
}

func TestIPage(t *testing.T) {
	records := []string{"item1", "item2"}
	page := NewIPage(records, 25, 1, 10)
	if page.Total != 25 || page.Pages != 3 || page.Size != 10 || page.Current != 1 {
		t.Fatalf("unexpected page calculation: %+v", page)
	}
}
