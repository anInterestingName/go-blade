// Package response 提供符合 SpringBlade / Saber 前端规范的统一 HTTP JSON 响应结构与输出辅助函数。
// 支持泛型数据载荷、分页 IPage 标准格式以及全局业务异常与 gRPC 异常的统一转换。
package response

import (
	"context"
	"net/http"

	"github.com/go-blade/pkg/trace"
	"github.com/go-blade/pkg/xerr"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Response 对标 SpringBlade 的统一响应实体 R<T>。
// 提供前端规范所需的 code、success、msg、data 四大核心字段。
type Response[T any] struct {
	// Code 业务状态码（200 代表成功，非 200 代表各类业务或系统错误）
	Code int `json:"code"`
	// Success 业务操作是否成功的布尔标识
	Success bool `json:"success"`
	// Msg 业务提示信息或异常描述
	Msg string `json:"msg"`
	// Data 泛型响应数据载荷
	Data T `json:"data"`
}

// IPage 对标 SpringBlade / MyBatis-Plus 的标准分页返回结构 IPage<T>。
// 完全兼容 Saber 前端表格组件的数据分页解析要求。
type IPage[T any] struct {
	// Records 当前页数据记录列表
	Records []T `json:"records"`
	// Total 数据记录总数
	Total int64 `json:"total"`
	// Size 每页展示记录上限
	Size int64 `json:"size"`
	// Current 当前页码（从 1 开始）
	Current int64 `json:"current"`
	// Pages 总页数计算值
	Pages int64 `json:"pages"`
}

// NewIPage 构造并计算包含分页统计信息的 IPage 实例。
// records: 当前页记录切片；total: 总条数；current: 当前页码；size: 每页显示条数。
func NewIPage[T any](records []T, total, current, size int64) *IPage[T] {
	if records == nil {
		records = make([]T, 0)
	}
	var pages int64
	if size > 0 {
		pages = (total + size - 1) / size
	}
	return &IPage[T]{
		Records: records,
		Total:   total,
		Size:    size,
		Current: current,
		Pages:   pages,
	}
}

// Success 构造一个成功状态的 Response 响应体。
// data: 泛型数据结果。
func Success[T any](data T) *Response[T] {
	return &Response[T]{
		Code:    xerr.OK,
		Success: true,
		Msg:     xerr.MapErrMsg(xerr.OK),
		Data:    data,
	}
}

// SuccessMsg 构造一个携带自定义成功提示语的 Response 响应体。
// msg: 成功提示语；data: 泛型数据结果。
func SuccessMsg[T any](msg string, data T) *Response[T] {
	return &Response[T]{
		Code:    xerr.OK,
		Success: true,
		Msg:     msg,
		Data:    data,
	}
}

// Fail 构造一个业务失败的 Response 响应体。
// code: 失败错误码；msg: 失败提示语。
func Fail(code int, msg string) *Response[any] {
	return &Response[any]{
		Code:    code,
		Success: false,
		Msg:     msg,
		Data:    nil,
	}
}

// OkJson 将成功的业务数据序列化为统一 JSON 格式并写入 HTTP 响应流。
func OkJson[T any](ctx context.Context, w http.ResponseWriter, data T) {
	httpx.OkJsonCtx(ctx, w, Success(data))
}

// OkJsonMsg 将携带自定义提示语的成功数据序列化为 JSON 写入 HTTP 响应流。
func OkJsonMsg[T any](ctx context.Context, w http.ResponseWriter, msg string, data T) {
	httpx.OkJsonCtx(ctx, w, SuccessMsg(msg, data))
}

// FailJson 将业务失败响应序列化为 JSON 写入 HTTP 响应流。
func FailJson(ctx context.Context, w http.ResponseWriter, code int, msg string) {
	httpx.OkJsonCtx(ctx, w, Fail(code, msg))
}

// FailJsonWithStatus 将业务失败响应以指定的 HTTP 状态码写入 HTTP 响应流。
// 例如鉴权拦截输出 HTTP 401 配合业务错误码。
func FailJsonWithStatus(ctx context.Context, w http.ResponseWriter, httpStatus int, code int, msg string) {
	httpx.WriteJsonCtx(ctx, w, httpStatus, Fail(code, msg))
}

// mapGrpcCodeToHTTP 将 gRPC 状态码安全映射为标准业务/HTTP 状态码。
func mapGrpcCodeToHTTP(c codes.Code) int {
	switch c {
	case codes.OK:
		return xerr.OK
	case codes.InvalidArgument:
		return xerr.RequestParamError
	case codes.Unauthenticated:
		return xerr.Unauthorized
	case codes.PermissionDenied:
		return xerr.Forbidden
	case codes.NotFound:
		return xerr.NotFound
	default:
		return xerr.ServerCommonError
	}
}

// ResultResponse 统一包装并输出 go-zero API Handler 的执行结果。
// 当 err 为 nil 时返回标准 200 成功响应；
// 当 err 为 *xerr.CodeError 时返回业务定义的错误码与提示；
// 当 err 为 gRPC Status 异常时映射为合规的业务错误码；
// 未知系统错误时自动在服务端记录详细日志，对外仅返回统一模糊提示，杜绝内部敏感信息泄露。
func ResultResponse(ctx context.Context, w http.ResponseWriter, resp any, err error) {
	// 自动从 Context 提取 Trace-ID 并写入 HTTP 响应头
	if traceID := trace.TraceIDFromContext(ctx); traceID != "" {
		w.Header().Set("X-Trace-Id", traceID)
	}

	if err == nil {
		httpx.OkJsonCtx(ctx, w, Success(resp))
		return
	}

	errCode := xerr.ServerCommonError
	errMsg := xerr.MapErrMsg(errCode)

	switch e := err.(type) {
	case *xerr.CodeError:
		errCode = e.GetCode()
		errMsg = e.GetMsg()
	default:
		// 支持 gRPC 错误解包与标准化映射
		if gstatus, ok := status.FromError(err); ok {
			if bizCode, bizMsg, isBiz := xerr.ParseGrpcBizError(gstatus.Message()); isBiz {
				errCode = bizCode
				errMsg = bizMsg
				if errMsg == "" {
					errMsg = xerr.MapErrMsg(errCode)
				}
			} else {
				errCode = mapGrpcCodeToHTTP(gstatus.Code())
				if errCode == xerr.ServerCommonError {
					// 服务端内部 gRPC 异常（如 codes.Internal / codes.Unknown / codes.Unavailable）：
					// 强制打点记录详细日志，对外脱敏返回统一安全文案，严禁将数据库报错或 panic 原文直出前端！
					logx.WithContext(ctx).Errorf("[gRPC-API-ERR] internal rpc error (code=%s): %s", gstatus.Code(), gstatus.Message())
					errMsg = xerr.MapErrMsg(xerr.ServerCommonError)
				} else {
					errMsg = gstatus.Message()
					if errMsg == "" {
						errMsg = xerr.MapErrMsg(errCode)
					}
				}
			}
		} else {
			// 未知内部系统异常：打点详细日志，对外统一返回安全文案，防止泄露内部敏感报错
			logx.WithContext(ctx).Errorf("[API-ERR] internal server error: %+v", err)
			errCode = xerr.ServerCommonError
			errMsg = xerr.MapErrMsg(xerr.ServerCommonError)
		}
	}

	httpx.OkJsonCtx(ctx, w, Fail(errCode, errMsg))
}
