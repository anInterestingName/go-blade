// Package oss 提供多租户隔离的统一对象存储抽象层，支持本地/内存存储驱动与标准 S3/MinIO 协议驱动。
package oss

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
)

const (
	// DriverLocal 本地文件系统存储驱动标识
	DriverLocal = "local"
	// DriverMemory 内存存储驱动标识（适用于单元测试或无状态临时存储）
	DriverMemory = "memory"
	// DriverS3 标准 AWS S3 协议存储驱动标识
	DriverS3 = "s3"
	// DriverMinIO MinIO 对象存储驱动标识（兼容 S3 V4 签名协议）
	DriverMinIO = "minio"

	// DefaultMaxFileSize 默认单文件最大上传字节数限制（20MB）
	DefaultMaxFileSize int64 = 20 * 1024 * 1024
	// DefaultBucketName 默认存储桶名称
	DefaultBucketName = "blade"
	// DefaultRegion 默认 S3 区域名称
	DefaultRegion = "us-east-1"
	// DefaultPresignExpires 默认预签名 URL 有效期（15 分钟）
	DefaultPresignExpires = 15 * time.Minute
)

// DefaultAllowedExts 默认允许上传的安全文件扩展名白名单（全小写，含点号）。
var DefaultAllowedExts = []string{
	".jpg", ".jpeg", ".png", ".gif", ".webp",
	".pdf", ".doc", ".docx", ".xls", ".xlsx",
	".txt", ".zip",
}

// Config 定义统一对象存储组件的配置项。
type Config struct {
	// Driver 存储驱动类型：可选 "local", "memory", "s3", "minio"
	Driver string `json:"driver,default=local"`
	// Endpoint 对象存储服务访问端点（如 "http://127.0.0.1:9000"）
	Endpoint string `json:"endpoint,optional"`
	// AccessKey 访问密钥 ID
	AccessKey string `json:"accessKey,optional"`
	// SecretKey 秘密访问密钥
	SecretKey string `json:"secretKey,optional"`
	// BucketName 存储桶名称
	BucketName string `json:"bucketName,default=blade"`
	// Region 存储桶所在区域
	Region string `json:"region,default=us-east-1"`
	// BaseDir 本地存储驱动的根目录路径（Driver="local" 时生效）
	BaseDir string `json:"baseDir,optional"`
	// MaxFileSize 允许上传的最大文件字节数（默认 20MB）
	MaxFileSize int64 `json:"maxFileSize,default=20971520"`
	// AllowedExts 允许上传的文件扩展名白名单
	AllowedExts []string `json:"allowedExts,optional"`
}

// normalize 补全 Config 的零值默认参数。
func (c Config) normalize() Config {
	cfg := c
	cfg.Driver = strings.ToLower(strings.TrimSpace(cfg.Driver))
	if cfg.Driver == "" {
		cfg.Driver = DriverLocal
	}
	cfg.Endpoint = strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	cfg.BucketName = strings.TrimSpace(cfg.BucketName)
	if cfg.BucketName == "" {
		cfg.BucketName = DefaultBucketName
	}
	cfg.Region = strings.TrimSpace(cfg.Region)
	if cfg.Region == "" {
		cfg.Region = DefaultRegion
	}
	if cfg.MaxFileSize <= 0 {
		cfg.MaxFileSize = DefaultMaxFileSize
	}
	if len(cfg.AllowedExts) == 0 {
		cfg.AllowedExts = append([]string(nil), DefaultAllowedExts...)
	} else {
		normalized := make([]string, 0, len(cfg.AllowedExts))
		for _, ext := range cfg.AllowedExts {
			e := strings.ToLower(strings.TrimSpace(ext))
			if e == "" {
				continue
			}
			if !strings.HasPrefix(e, ".") {
				e = "." + e
			}
			normalized = append(normalized, e)
		}
		if len(normalized) == 0 {
			normalized = append([]string(nil), DefaultAllowedExts...)
		}
		cfg.AllowedExts = normalized
	}
	return cfg
}

// OssFile 对标 SpringBlade BladeFile 的对象存储上传返回实体。
type OssFile struct {
	// Link 完整可访问的资源 URL
	Link string `json:"link"`
	// Domain 存储服务的域名或端点
	Domain string `json:"domain"`
	// Name 存储后的多租户隔离对象 Key（如 "000000/20261026/1850123456789_avatar.png"）
	Name string `json:"name"`
	// OriginalName 客户端上传时的原始文件名
	OriginalName string `json:"original_name"`
	// Size 文件字节大小
	Size int64 `json:"size"`
	// ContentType 文件 MIME 类型
	ContentType string `json:"content_type"`
}

// PresignedURLData 封装预签名 URL 接口响应载体。
type PresignedURLData struct {
	// ObjectKey 多租户隔离的对象存储 Key
	ObjectKey string `json:"object_key"`
	// URL 预签名上传或下载链接
	URL string `json:"url"`
	// Method 对应的操作类型（"PUT" 或 "GET"）
	Method string `json:"method"`
	// Expires 有效期秒数
	Expires int64 `json:"expires"`
}

// Provider 定义统一对象存储抽象接口。
type Provider interface {
	// PutObject 流式上传对象并返回包含访问链接与元信息的 OssFile
	PutObject(ctx context.Context, originalFilename string, reader io.Reader, size int64, contentType string) (*OssFile, error)
	// GetObject 根据对象 Key 获取对象内容流（调用方负责 Close）
	GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error)
	// DeleteObject 根据对象 Key 删除存储对象
	DeleteObject(ctx context.Context, objectKey string) error
	// PresignedPutURL 生成客户端直传（PUT）预签名 URL 及分配的多租户对象 Key
	PresignedPutURL(ctx context.Context, originalFilename string, expires time.Duration) (objectKey string, uploadURL string, err error)
	// PresignedGetURL 生成受限对象临时下载（GET）预签名 URL
	PresignedGetURL(ctx context.Context, objectKey string, expires time.Duration) (downloadURL string, err error)
}

// NewProvider 根据配置初始化对应的对象存储驱动实例。
func NewProvider(cfg Config) (Provider, error) {
	normalized := cfg.normalize()
	switch normalized.Driver {
	case DriverLocal, DriverMemory:
		return NewLocalProvider(normalized)
	case DriverS3, DriverMinIO:
		return NewS3Provider(normalized)
	default:
		return nil, xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("不支持的对象存储驱动类型: %s", cfg.Driver))
	}
}

// BuildTenantObjectKey 使用默认扩展名白名单生成多租户安全隔离的对象存储路径。
// 格式为：<tenant_id>/<YYYYMMDD>/<snowflake_id>_<clean_filename>。
func BuildTenantObjectKey(ctx context.Context, originalFilename string) (string, error) {
	return buildTenantObjectKeyWithExts(ctx, originalFilename, DefaultAllowedExts)
}

// buildTenantObjectKeyWithExts 根据指定的扩展名白名单校验文件名并生成多租户安全对象 Key。
func buildTenantObjectKeyWithExts(ctx context.Context, originalFilename string, allowedExts []string) (string, error) {
	cleanName, err := sanitizeFilename(originalFilename, allowedExts)
	if err != nil {
		return "", err
	}

	tenantID := sanitizePathSegment(tenant.FromTenant(ctx))
	if tenantID == "" {
		tenantID = tenant.DefaultTenantID
	}

	dateStr := time.Now().Format("20060102")
	idStr := snowflake.GenIDString()
	return fmt.Sprintf("%s/%s/%s_%s", tenantID, dateStr, idStr, cleanName), nil
}

// sanitizeFilename 清理原始文件名以防御路径穿越攻击，并校验扩展名是否在允许的白名单内。
func sanitizeFilename(originalFilename string, allowedExts []string) (string, error) {
	trimmed := strings.TrimSpace(originalFilename)
	if trimmed == "" {
		return "", xerr.NewCodeError(xerr.RequestParamError, "文件名不能为空")
	}
	if strings.ContainsRune(trimmed, '\x00') {
		return "", xerr.NewCodeError(xerr.RequestParamError, "文件名包含非法字符")
	}

	// 统一将反斜杠替换为正斜杠后提取基础文件名，彻底剥离任何 ../ 或目录前缀
	normalized := strings.ReplaceAll(trimmed, "\\", "/")
	base := path.Base(normalized)
	base = filepath.Base(base)
	base = strings.TrimSpace(base)

	if base == "" || base == "." || base == ".." || strings.Contains(base, "/") || strings.Contains(base, "\\") {
		return "", xerr.NewCodeError(xerr.RequestParamError, "非法的文件名")
	}

	// 清理可能导致路径或 URL 注入的特殊控制字符
	var sb strings.Builder
	for _, r := range base {
		if r < 32 || r == 127 || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
			sb.WriteByte('_')
		} else {
			sb.WriteRune(r)
		}
	}
	cleanName := strings.TrimSpace(sb.String())
	if cleanName == "" || cleanName == "." || cleanName == ".." || strings.Contains(cleanName, "..") || strings.HasPrefix(cleanName, ".") {
		return "", xerr.NewCodeError(xerr.RequestParamError, "非法的文件名")
	}

	ext := strings.ToLower(path.Ext(cleanName))
	stem := strings.TrimSpace(strings.TrimSuffix(cleanName, path.Ext(cleanName)))
	if stem == "" || ext == "" || !isExtAllowed(ext, allowedExts) {
		return "", xerr.NewCodeError(xerr.RequestParamError, "不支持的文件类型")
	}

	return cleanName, nil
}

// isExtAllowed 检查文件扩展名是否在白名单内。
func isExtAllowed(ext string, allowedExts []string) bool {
	ext = strings.ToLower(strings.TrimSpace(ext))
	if ext == "" {
		return false
	}
	if len(allowedExts) == 0 {
		allowedExts = DefaultAllowedExts
	}
	for _, allowed := range allowedExts {
		a := strings.ToLower(strings.TrimSpace(allowed))
		if !strings.HasPrefix(a, ".") {
			a = "." + a
		}
		if ext == a {
			return true
		}
	}
	return false
}

// sanitizePathSegment 清理租户 ID 等路径段，防止通过伪造 Header 注入 ../。
func sanitizePathSegment(seg string) string {
	seg = strings.TrimSpace(seg)
	if seg == "" {
		return ""
	}
	var sb strings.Builder
	for _, r := range seg {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// cleanObjectKey 校验并规范化对象存储 Key，拒绝空路径或包含 .. 的路径穿越请求。
func cleanObjectKey(objectKey string) (string, error) {
	trimmed := strings.TrimSpace(objectKey)
	if trimmed == "" || strings.ContainsRune(trimmed, '\x00') {
		return "", xerr.NewCodeError(xerr.RequestParamError, "无效的对象存储路径")
	}
	normalized := strings.ReplaceAll(trimmed, "\\", "/")
	for _, part := range strings.Split(normalized, "/") {
		if part == ".." || part == "." {
			return "", xerr.NewCodeError(xerr.RequestParamError, "对象路径包含非法的路径穿越字符")
		}
	}
	cleaned := strings.TrimPrefix(path.Clean(normalized), "/")
	if cleaned == "" || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", xerr.NewCodeError(xerr.RequestParamError, "无效的对象存储路径")
	}
	return cleaned, nil
}

// readBounded 流式读取数据并严格校验文件大小上限（防止客户端伪造 size 绕过限制）。
func readBounded(reader io.Reader, declaredSize int64, maxFileSize int64) ([]byte, error) {
	if maxFileSize <= 0 {
		maxFileSize = DefaultMaxFileSize
	}
	if declaredSize > maxFileSize {
		return nil, xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("文件大小超出限制（最大允许 %d 字节）", maxFileSize))
	}
	if reader == nil {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "文件内容不能为空")
	}

	limited := io.LimitReader(reader, maxFileSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxFileSize {
		return nil, xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("文件大小超出限制（最大允许 %d 字节）", maxFileSize))
	}
	return data, nil
}

// resolveContentType 推断文件的 MIME Content-Type。
func resolveContentType(contentType string, filename string, data []byte) string {
	ct := strings.TrimSpace(contentType)
	if ct != "" && ct != "application/octet-stream" {
		return ct
	}
	if ext := strings.ToLower(path.Ext(filename)); ext != "" {
		if byExt := mime.TypeByExtension(ext); byExt != "" {
			return byExt
		}
	}
	if len(data) > 0 {
		return http.DetectContentType(data)
	}
	return "application/octet-stream"
}

// UploadHTTPHandler 构造通用的表单文件上传 HTTP 处理函数（对标 /blade-resource/oss/endpoint/put-file）。
// 解析 multipart/form-data 中的 file 字段，调用 provider.PutObject，并输出统一 R<OssFile> JSON 响应。
func UploadHTTPHandler(provider Provider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if r.Method != http.MethodPost {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.MethodNotAllowed, "仅支持 POST 上传文件"))
			return
		}
		if provider == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewErrMsg("对象存储服务未初始化"))
			return
		}

		// 若请求头携带 Blade-Tenant-Id 且上下文尚未注入，则自动提取到上下文中
		if headerTenant := strings.TrimSpace(r.Header.Get(tenant.HeaderTenantID)); headerTenant != "" && tenant.FromTenant(ctx) == tenant.DefaultTenantID {
			ctx = tenant.WithTenant(ctx, headerTenant)
		}

		if err := r.ParseMultipartForm(DefaultMaxFileSize); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "解析上传表单失败，请确认使用 multipart/form-data 格式且文件未超限"))
			return
		}
		if r.MultipartForm != nil {
			defer func() {
				_ = r.MultipartForm.RemoveAll()
			}()
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "未找到上传文件字段 file"))
			return
		}
		defer func() {
			_ = file.Close()
		}()

		contentType := header.Header.Get("Content-Type")
		ossFile, err := provider.PutObject(ctx, header.Filename, file, header.Size, contentType)
		response.ResultResponse(ctx, w, ossFile, err)
	}
}

// PresignedURLHTTPHandler 构造通用的预签名 URL 获取 HTTP 处理函数（对标 /blade-resource/oss/endpoint/presigned-url）。
// 支持查询参数：
// - method: "PUT"（默认，需传 filename）或 "GET"（需传 key / objectKey / name）
// - filename: 原始文件名（PUT 时必填）
// - key / objectKey / name: 已存储的对象 Key（GET 时必填）
// - expires: 过期秒数（可选，默认 900 秒）
func PresignedURLHTTPHandler(provider Provider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.MethodNotAllowed, "不支持的请求方法"))
			return
		}
		if provider == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewErrMsg("对象存储服务未初始化"))
			return
		}

		if headerTenant := strings.TrimSpace(r.Header.Get(tenant.HeaderTenantID)); headerTenant != "" && tenant.FromTenant(ctx) == tenant.DefaultTenantID {
			ctx = tenant.WithTenant(ctx, headerTenant)
		}

		query := r.URL.Query()
		method := strings.ToUpper(strings.TrimSpace(query.Get("method")))
		if method == "" {
			if query.Get("key") != "" || query.Get("objectKey") != "" {
				method = http.MethodGet
			} else {
				method = http.MethodPut
			}
		}

		expires := DefaultPresignExpires
		if expStr := strings.TrimSpace(query.Get("expires")); expStr != "" {
			sec, err := strconv.ParseInt(expStr, 10, 64)
			if err != nil || sec <= 0 {
				response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "无效的 expires 参数"))
				return
			}
			expires = time.Duration(sec) * time.Second
		}

		switch method {
		case http.MethodPut:
			filename := strings.TrimSpace(query.Get("filename"))
			if filename == "" {
				filename = strings.TrimSpace(query.Get("originalFilename"))
			}
			if filename == "" {
				response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "缺少 filename 参数"))
				return
			}
			objectKey, uploadURL, err := provider.PresignedPutURL(ctx, filename, expires)
			if err != nil {
				response.ResultResponse(ctx, w, nil, err)
				return
			}
			response.ResultResponse(ctx, w, &PresignedURLData{
				ObjectKey: objectKey,
				URL:       uploadURL,
				Method:    http.MethodPut,
				Expires:   int64(expires.Seconds()),
			}, nil)

		case http.MethodGet:
			objectKey := strings.TrimSpace(query.Get("objectKey"))
			if objectKey == "" {
				objectKey = strings.TrimSpace(query.Get("key"))
			}
			if objectKey == "" {
				objectKey = strings.TrimSpace(query.Get("name"))
			}
			if objectKey == "" {
				response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "缺少 objectKey 参数"))
				return
			}
			downloadURL, err := provider.PresignedGetURL(ctx, objectKey, expires)
			if err != nil {
				response.ResultResponse(ctx, w, nil, err)
				return
			}
			response.ResultResponse(ctx, w, &PresignedURLData{
				ObjectKey: objectKey,
				URL:       downloadURL,
				Method:    http.MethodGet,
				Expires:   int64(expires.Seconds()),
			}, nil)

		default:
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "method 仅支持 PUT 或 GET"))
		}
	}
}
