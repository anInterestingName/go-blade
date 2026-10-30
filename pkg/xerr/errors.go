// Package xerr 定义全局统一业务异常与错误码规范。
// 对标 SpringBlade 的 ResultCode 体系，为微服务与网关提供标准化的错误码与文案映射。
package xerr

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const grpcBizPrefix = "BIZ_ERR:"

// CodeError 业务异常结构体，用于在业务逻辑中携带错误码与用户可读的提示信息。
// 兼容 SpringBlade 错误响应规范，支持在 Handler 中统一转换为 HTTP 响应。
type CodeError struct {
	// Code 业务错误码（如 200, 400, 500 或 40001 等业务子错误码）
	Code int `json:"code"`
	// Msg 用户可读的业务错误描述信息
	Msg string `json:"msg"`
}

// Error 实现 Go 标准库 error 接口，输出包含错误码与提示文案的格式化字符串。
func (e *CodeError) Error() string {
	return fmt.Sprintf("ErrCode:%d，ErrMsg:%s", e.Code, e.Msg)
}

// GetCode 获取当前异常所携带的业务错误码。
func (e *CodeError) GetCode() int {
	return e.Code
}

// GetMsg 获取当前异常所携带的错误提示文案。
func (e *CodeError) GetMsg() string {
	return e.Msg
}

// NewCodeError 构造并返回一个包含指定错误码和提示文案的 CodeError 实例。
// code: 业务错误码；msg: 错误描述文本。
func NewCodeError(code int, msg string) *CodeError {
	return &CodeError{Code: code, Msg: msg}
}

// NewErrMsg 构造并返回一个默认 500 服务器错误码的 CodeError 实例。
// msg: 自定义错误提示文本。
func NewErrMsg(msg string) *CodeError {
	return &CodeError{Code: ServerCommonError, Msg: msg}
}

// ToGrpcError 将业务错误或系统错误转换为标准 gRPC Status 错误。
// 若 err 为 *CodeError 且属于非 500 业务异常，会在 gRPC status message 中编码业务错误码，以便网关层无损还原；
// 若为 500 系统异常或未知错误，则转换为 codes.Internal。
func ToGrpcError(err error) error {
	if err == nil {
		return nil
	}
	if ce, ok := err.(*CodeError); ok {
		grpcCode := mapBizCodeToGrpc(ce.Code)
		if ce.Code == ServerCommonError {
			return status.Error(codes.Internal, ce.Msg)
		}
		return status.Error(grpcCode, fmt.Sprintf("%s%d:%s", grpcBizPrefix, ce.Code, ce.Msg))
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

// ParseGrpcBizError 尝试从 gRPC status message 中解析出编码的业务错误码与消息。
func ParseGrpcBizError(msg string) (int, string, bool) {
	if !strings.HasPrefix(msg, grpcBizPrefix) {
		return 0, "", false
	}
	rest := strings.TrimPrefix(msg, grpcBizPrefix)
	idx := strings.IndexByte(rest, ':')
	if idx <= 0 {
		return 0, "", false
	}
	code, err := strconv.Atoi(rest[:idx])
	if err != nil {
		return 0, "", false
	}
	return code, rest[idx+1:], true
}

func mapBizCodeToGrpc(code int) codes.Code {
	switch code {
	case OK:
		return codes.OK
	case RequestParamError, CaptchaError:
		return codes.InvalidArgument
	case Unauthorized, TokenExpired, TokenInvalid, PasswordError:
		return codes.Unauthenticated
	case Forbidden, TenantNotFound:
		return codes.PermissionDenied
	case NotFound, UserNotFound, DataNotExist:
		return codes.NotFound
	case DataAlreadyExist:
		return codes.AlreadyExists
	default:
		return codes.Internal
	}
}

// 常用 HTTP 与通用业务错误码定义（对标 SpringBlade ResultCode）
const (
	// OK 代表操作成功 (200)
	OK = 200
	// RequestParamError 代表请求参数有误或校验未通过 (400)
	RequestParamError = 400
	// Unauthorized 代表未携带身份凭证或登录已失效 (401)
	Unauthorized = 401
	// Forbidden 代表当前用户无权限访问目标资源 (403)
	Forbidden = 403
	// NotFound 代表请求的目标资源或路由不存在 (404)
	NotFound = 404
	// MethodNotAllowed 代表 HTTP 请求方法不支持 (405)
	MethodNotAllowed = 405
	// ServerCommonError 代表服务端发生未捕获的内部异常 (500)
	ServerCommonError = 500

	// TokenExpired 代表用户访问凭证已过期 (40001)
	TokenExpired = 40001
	// TokenInvalid 代表访问凭证签名无效或格式非法 (40002)
	TokenInvalid = 40002
	// TenantNotFound 代表请求指定的租户不存在或已被禁用 (40003)
	TenantNotFound = 40003
	// UserNotFound 代表指定的用户账号不存在 (40004)
	UserNotFound = 40004
	// PasswordError 代表用户密码输入错误 (40005)
	PasswordError = 40005
	// CaptchaError 代表图形验证码或短信验证码校验失败 (40006)
	CaptchaError = 40006
	// DataNotExist 代表业务查询的主体记录在数据源中不存在 (40007)
	DataNotExist = 40007
	// DataAlreadyExist 代表唯一索引冲突或业务数据重复操作 (40008)
	DataAlreadyExist = 40008
)

// codeMsgMap 维护错误码与默认标准中文提示语的映射关系字典
var codeMsgMap = map[int]string{
	OK:                "操作成功",
	RequestParamError: "请求参数错误",
	Unauthorized:      "请求未授权",
	Forbidden:         "无权访问当前资源",
	NotFound:          "请求资源不存在",
	MethodNotAllowed:  "请求方法不允许",
	ServerCommonError: "服务器内部错误",

	TokenExpired:     "Token已过期，请重新登录",
	TokenInvalid:     "无效的访问令牌",
	TenantNotFound:   "租户不存在或已被禁用",
	UserNotFound:     "用户不存在",
	PasswordError:    "用户名或密码错误",
	CaptchaError:     "验证码错误或已失效",
	DataNotExist:     "查询数据不存在",
	DataAlreadyExist: "数据已存在，请勿重复操作",
}

// MapErrMsg 根据错误码获取标准错误提示。
// 如果传入的 errCode 未在字典中注册，则兜底返回友好的默认提示。
func MapErrMsg(errCode int) string {
	if msg, ok := codeMsgMap[errCode]; ok {
		return msg
	}
	return "服务器开小差了，请稍后再试"
}
