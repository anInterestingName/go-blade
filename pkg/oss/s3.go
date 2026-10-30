// Package oss 提供多租户隔离的统一对象存储抽象层，支持本地/内存存储驱动与标准 S3/MinIO 协议驱动。
package oss

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-blade/pkg/xerr"
)

const (
	awsV4Algorithm   = "AWS4-HMAC-SHA256"
	awsV4Service     = "s3"
	awsV4RequestType = "aws4_request"
	unsignedPayload  = "UNSIGNED-PAYLOAD"
)

// S3Provider 实现基于标准 HTTP 与 AWS Signature Version 4 的 S3 / MinIO 存储驱动。
// 零第三方 SDK 依赖，100% 兼容 MinIO、AWS S3 及阿里云 OSS 等 S3 协议兼容存储。
type S3Provider struct {
	cfg        Config
	endpoint   *url.URL
	httpClient *http.Client
	nowFunc    func() time.Time
}

// NewS3Provider 构造并返回一个 S3Provider 实例。
func NewS3Provider(cfg Config) (*S3Provider, error) {
	normalized := cfg.normalize()
	if normalized.Endpoint == "" {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "S3/MinIO 存储驱动缺少 endpoint 配置")
	}
	parsedURL, err := url.Parse(normalized.Endpoint)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("无效的 S3 endpoint 地址: %s", normalized.Endpoint))
	}
	if normalized.AccessKey == "" {
		normalized.AccessKey = "minioadmin"
	}
	if normalized.SecretKey == "" {
		normalized.SecretKey = "minioadmin"
	}

	return &S3Provider{
		cfg:      normalized,
		endpoint: parsedURL,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		nowFunc: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

// PutObject 使用 AWS Signature Version 4 签名向 S3/MinIO 发起 PUT Object 请求。
func (p *S3Provider) PutObject(ctx context.Context, originalFilename string, reader io.Reader, size int64, contentType string) (*OssFile, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	objectKey, err := buildTenantObjectKeyWithExts(ctx, originalFilename, p.cfg.AllowedExts)
	if err != nil {
		return nil, err
	}

	data, err := readBounded(reader, size, p.cfg.MaxFileSize)
	if err != nil {
		return nil, err
	}

	actualSize := int64(len(data))
	resolvedCT := resolveContentType(contentType, originalFilename, data)

	targetURL, canonicalURI := p.buildObjectURL(objectKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	req.ContentLength = actualSize
	req.Header.Set("Content-Type", resolvedCT)
	p.signRequestV4(req, canonicalURI, data)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("s3 PutObject failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	return &OssFile{
		Link:         targetURL,
		Domain:       p.cfg.Endpoint,
		Name:         objectKey,
		OriginalName: originalFilename,
		Size:         actualSize,
		ContentType:  resolvedCT,
	}, nil
}

// GetObject 使用 AWS Signature Version 4 签名向 S3/MinIO 发起 GET Object 请求并返回响应流。
func (p *S3Provider) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	cleanedKey, err := cleanObjectKey(objectKey)
	if err != nil {
		return nil, err
	}

	targetURL, canonicalURI := p.buildObjectURL(cleanedKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}

	p.signRequestV4(req, canonicalURI, nil)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, xerr.NewCodeError(xerr.NotFound, "请求的对象文件不存在")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("s3 GetObject failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	return resp.Body, nil
}

// DeleteObject 使用 AWS Signature Version 4 签名向 S3/MinIO 发起 DELETE Object 请求。
func (p *S3Provider) DeleteObject(ctx context.Context, objectKey string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	cleanedKey, err := cleanObjectKey(objectKey)
	if err != nil {
		return err
	}

	targetURL, canonicalURI := p.buildObjectURL(cleanedKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, targetURL, nil)
	if err != nil {
		return err
	}

	p.signRequestV4(req, canonicalURI, nil)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("s3 DeleteObject failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}
	return nil
}

// PresignedPutURL 生成多租户隔离的对象 Key 以及标准 S3 Query-String Presigned PUT URL（AWS4-HMAC-SHA256）。
func (p *S3Provider) PresignedPutURL(ctx context.Context, originalFilename string, expires time.Duration) (string, string, error) {
	if ctx != nil && ctx.Err() != nil {
		return "", "", ctx.Err()
	}

	objectKey, err := buildTenantObjectKeyWithExts(ctx, originalFilename, p.cfg.AllowedExts)
	if err != nil {
		return "", "", err
	}
	signedURL, err := p.presignURL(http.MethodPut, objectKey, expires)
	if err != nil {
		return "", "", err
	}
	return objectKey, signedURL, nil
}

// PresignedGetURL 生成指定对象的标准 S3 Query-String Presigned GET URL（AWS4-HMAC-SHA256）。
func (p *S3Provider) PresignedGetURL(ctx context.Context, objectKey string, expires time.Duration) (string, error) {
	if ctx != nil && ctx.Err() != nil {
		return "", ctx.Err()
	}

	cleanedKey, err := cleanObjectKey(objectKey)
	if err != nil {
		return "", err
	}
	return p.presignURL(http.MethodGet, cleanedKey, expires)
}

// buildObjectURL 构建 Path-Style 的 S3 对象完整 URL 与规范化 URI 路径。
func (p *S3Provider) buildObjectURL(objectKey string) (fullURL string, canonicalURI string) {
	basePath := strings.TrimRight(p.endpoint.Path, "/")
	escapedKey := escapeObjectKeyPath(objectKey)
	canonicalURI = fmt.Sprintf("%s/%s/%s", basePath, s3URIEscape(p.cfg.BucketName, true), escapedKey)
	if !strings.HasPrefix(canonicalURI, "/") {
		canonicalURI = "/" + canonicalURI
	}
	fullURL = fmt.Sprintf("%s://%s%s", p.endpoint.Scheme, p.endpoint.Host, canonicalURI)
	return fullURL, canonicalURI
}

// signRequestV4 对标准 HTTP 请求注入 AWS Signature Version 4 请求头（Authorization, X-Amz-Date, X-Amz-Content-Sha256）。
func (p *S3Provider) signRequestV4(req *http.Request, canonicalURI string, payload []byte) {
	now := p.nowFunc().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := sha256Hex(payload)
	host := p.endpoint.Host
	req.Host = host
	req.Header.Set("Host", host)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	headersToSign := map[string]string{
		"host":                 host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	if ct := strings.TrimSpace(req.Header.Get("Content-Type")); ct != "" {
		headersToSign["content-type"] = ct
	}

	var signedHeaderNames []string
	for k := range headersToSign {
		signedHeaderNames = append(signedHeaderNames, k)
	}
	sort.Strings(signedHeaderNames)

	var canonicalHeaders strings.Builder
	for _, k := range signedHeaderNames {
		canonicalHeaders.WriteString(k)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.TrimSpace(headersToSign[k]))
		canonicalHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(signedHeaderNames, ";")

	canonicalQuery := buildCanonicalQueryString(req.URL.Query())
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := fmt.Sprintf("%s/%s/%s/%s", dateStamp, p.cfg.Region, awsV4Service, awsV4RequestType)
	stringToSign := strings.Join([]string{
		awsV4Algorithm,
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := deriveSigningKey(p.cfg.SecretKey, dateStamp, p.cfg.Region, awsV4Service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	authHeader := fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		awsV4Algorithm,
		p.cfg.AccessKey,
		credentialScope,
		signedHeaders,
		signature,
	)
	req.Header.Set("Authorization", authHeader)
}

// presignURL 生成符合 AWS Signature Version 4 规范的 Query-String 预签名 URL。
func (p *S3Provider) presignURL(method string, objectKey string, expires time.Duration) (string, error) {
	if expires <= 0 {
		expires = DefaultPresignExpires
	}
	expSeconds := int64(expires.Seconds())
	if expSeconds <= 0 {
		expSeconds = 1
	}
	if expSeconds > 604800 {
		expSeconds = 604800 // S3 V4 预签名最长 7 天
	}

	now := p.nowFunc().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	credentialScope := fmt.Sprintf("%s/%s/%s/%s", dateStamp, p.cfg.Region, awsV4Service, awsV4RequestType)

	targetURL, canonicalURI := p.buildObjectURL(objectKey)
	query := url.Values{}
	query.Set("X-Amz-Algorithm", awsV4Algorithm)
	query.Set("X-Amz-Credential", fmt.Sprintf("%s/%s", p.cfg.AccessKey, credentialScope))
	query.Set("X-Amz-Date", amzDate)
	query.Set("X-Amz-Expires", strconv.FormatInt(expSeconds, 10))
	query.Set("X-Amz-SignedHeaders", "host")

	canonicalQuery := buildCanonicalQueryString(query)
	canonicalHeaders := fmt.Sprintf("host:%s\n", p.endpoint.Host)
	signedHeaders := "host"

	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		unsignedPayload,
	}, "\n")

	stringToSign := strings.Join([]string{
		awsV4Algorithm,
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := deriveSigningKey(p.cfg.SecretKey, dateStamp, p.cfg.Region, awsV4Service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	return fmt.Sprintf("%s?%s&X-Amz-Signature=%s", targetURL, canonicalQuery, signature), nil
}

// escapeObjectKeyPath 按 AWS S3 V4 URI 编码规范对 objectKey 的各路径段进行转义（保留 '/'）。
func escapeObjectKeyPath(objectKey string) string {
	return s3URIEscape(objectKey, false)
}

// s3URIEscape 严格遵循 AWS Signature V4 UriEncode 规范。
func s3URIEscape(input string, encodeSlash bool) string {
	var sb strings.Builder
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
			ch == '_' || ch == '-' || ch == '~' || ch == '.' {
			sb.WriteByte(ch)
		} else if ch == '/' && !encodeSlash {
			sb.WriteByte('/')
		} else {
			sb.WriteString(fmt.Sprintf("%%%02X", ch))
		}
	}
	return sb.String()
}

// buildCanonicalQueryString 构建按字节序排序的 AWS V4 规范化查询字符串。
func buildCanonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	type kv struct {
		k string
		v string
	}
	var pairs []kv
	for k, vs := range values {
		escapedKey := s3URIEscape(k, true)
		if len(vs) == 0 {
			pairs = append(pairs, kv{k: escapedKey, v: ""})
			continue
		}
		for _, v := range vs {
			pairs = append(pairs, kv{k: escapedKey, v: s3URIEscape(v, true)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k == pairs[j].k {
			return pairs[i].v < pairs[j].v
		}
		return pairs[i].k < pairs[j].k
	})

	parts := make([]string, 0, len(pairs))
	for _, p := range pairs {
		parts = append(parts, p.k+"="+p.v)
	}
	return strings.Join(parts, "&")
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}

func deriveSigningKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte(awsV4RequestType))
	return kSigning
}
