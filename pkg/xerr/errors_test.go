package xerr

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCodeError(t *testing.T) {
	err := NewCodeError(UserNotFound, MapErrMsg(UserNotFound))
	if err.GetCode() != UserNotFound {
		t.Fatalf("expected code %d, got %d", UserNotFound, err.GetCode())
	}
	if err.GetMsg() != "用户不存在" {
		t.Fatalf("expected msg 用户不存在, got %s", err.GetMsg())
	}
	if err.Error() == "" {
		t.Fatal("expected non-empty Error() string")
	}

	commonErr := NewErrMsg("数据库连接超时")
	if commonErr.GetCode() != ServerCommonError {
		t.Fatalf("expected %d, got %d", ServerCommonError, commonErr.GetCode())
	}
	if MapErrMsg(999999) == "" {
		t.Fatal("expected fallback message for unknown code")
	}
}

func TestToGrpcError_And_ParseGrpcBizError(t *testing.T) {
	if ToGrpcError(nil) != nil {
		t.Fatal("expected nil for nil error")
	}

	// 1. 业务错误双向无损编解码
	cases := []struct {
		bizCode      int
		expectedGrpc codes.Code
	}{
		{OK, codes.OK},
		{RequestParamError, codes.InvalidArgument},
		{CaptchaError, codes.InvalidArgument},
		{Unauthorized, codes.Unauthenticated},
		{TokenExpired, codes.Unauthenticated},
		{TokenInvalid, codes.Unauthenticated},
		{PasswordError, codes.Unauthenticated},
		{Forbidden, codes.PermissionDenied},
		{TenantNotFound, codes.PermissionDenied},
		{NotFound, codes.NotFound},
		{UserNotFound, codes.NotFound},
		{DataNotExist, codes.NotFound},
		{DataAlreadyExist, codes.AlreadyExists},
	}

	for _, tc := range cases {
		orig := NewCodeError(tc.bizCode, MapErrMsg(tc.bizCode))
		grpcErr := ToGrpcError(orig)
		st, ok := status.FromError(grpcErr)
		if !ok || st.Code() != tc.expectedGrpc {
			t.Fatalf("code %d expected grpc %v, got %v", tc.bizCode, tc.expectedGrpc, st.Code())
		}
		if tc.bizCode != ServerCommonError && tc.bizCode != OK {
			parsedCode, parsedMsg, ok := ParseGrpcBizError(st.Message())
			if !ok || parsedCode != tc.bizCode || parsedMsg != orig.GetMsg() {
				t.Fatalf("ParseGrpcBizError mismatch for %d: ok=%v code=%d msg=%s", tc.bizCode, ok, parsedCode, parsedMsg)
			}
		}
	}

	// 2. 500 错误与未知错误转换为 Internal 且不带 BIZ_ERR 前缀
	sysGrpc := ToGrpcError(NewErrMsg("internal sql panic"))
	stSys, _ := status.FromError(sysGrpc)
	if stSys.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v", stSys.Code())
	}
	if _, _, ok := ParseGrpcBizError(stSys.Message()); ok {
		t.Fatal("500 internal error should not be parsed as biz error")
	}

	rawGrpc := ToGrpcError(errors.New("raw system error"))
	stRaw, _ := status.FromError(rawGrpc)
	if stRaw.Code() != codes.Internal {
		t.Fatalf("expected Internal for raw error, got %v", stRaw.Code())
	}
}
