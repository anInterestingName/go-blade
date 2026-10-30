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
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-blade/pkg/xerr"
)

type memoryObject struct {
	data        []byte
	contentType string
}

// LocalProvider 实现基于本地文件系统或纯内存的对象存储驱动。
// 当 Config.Driver == "memory" 或 Config.BaseDir 为空时默认使用内存存储；
// 当配置了有效的 BaseDir 且 Driver != "memory" 时将对象持久化至本地目录。
type LocalProvider struct {
	cfg     Config
	mu      sync.RWMutex
	objects map[string]memoryObject
}

// NewLocalProvider 构造并返回一个 LocalProvider 实例。
func NewLocalProvider(cfg Config) (*LocalProvider, error) {
	normalized := cfg.normalize()
	if normalized.Endpoint == "" {
		normalized.Endpoint = "http://127.0.0.1:8000"
	}
	if normalized.SecretKey == "" {
		normalized.SecretKey = "blade-local-oss-secret-key"
	}
	if normalized.AccessKey == "" {
		normalized.AccessKey = "blade-local"
	}

	if normalized.Driver == DriverLocal && strings.TrimSpace(normalized.BaseDir) != "" {
		if err := os.MkdirAll(normalized.BaseDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create local oss base directory: %w", err)
		}
	}

	return &LocalProvider{
		cfg:     normalized,
		objects: make(map[string]memoryObject),
	}, nil
}

// useFilesystem 判断当前 LocalProvider 是否启用本地磁盘存储。
func (p *LocalProvider) useFilesystem() bool {
	return p.cfg.Driver != DriverMemory && strings.TrimSpace(p.cfg.BaseDir) != ""
}

// resolveLocalFilePath 将对象 Key 解析为安全的本地文件系统路径，严格防御目录穿越逃逸。
func (p *LocalProvider) resolveLocalFilePath(objectKey string) (string, error) {
	cleanedKey, err := cleanObjectKey(objectKey)
	if err != nil {
		return "", err
	}
	baseAbs, err := filepath.Abs(p.cfg.BaseDir)
	if err != nil {
		return "", err
	}
	targetAbs, err := filepath.Abs(filepath.Join(baseAbs, filepath.FromSlash(cleanedKey)))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(baseAbs, targetAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", xerr.NewCodeError(xerr.RequestParamError, "非法的本地存储对象路径")
	}
	return targetAbs, nil
}

// PutObject 将文件流保存至内存或本地磁盘，并返回标准 OssFile 结构。
func (p *LocalProvider) PutObject(ctx context.Context, originalFilename string, reader io.Reader, size int64, contentType string) (*OssFile, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
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

	if p.useFilesystem() {
		fullPath, err := p.resolveLocalFilePath(objectKey)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(fullPath, data, 0o644); err != nil {
			return nil, err
		}
	} else {
		p.mu.Lock()
		p.objects[objectKey] = memoryObject{
			data:        data,
			contentType: resolvedCT,
		}
		p.mu.Unlock()
	}

	link := fmt.Sprintf("%s/%s/%s", p.cfg.Endpoint, p.cfg.BucketName, objectKey)
	return &OssFile{
		Link:         link,
		Domain:       p.cfg.Endpoint,
		Name:         objectKey,
		OriginalName: originalFilename,
		Size:         actualSize,
		ContentType:  resolvedCT,
	}, nil
}

// GetObject 从内存或本地磁盘读取指定 objectKey 的数据流。
func (p *LocalProvider) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	cleanedKey, err := cleanObjectKey(objectKey)
	if err != nil {
		return nil, err
	}

	if p.useFilesystem() {
		fullPath, err := p.resolveLocalFilePath(cleanedKey)
		if err != nil {
			return nil, err
		}
		f, err := os.Open(fullPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, xerr.NewCodeError(xerr.NotFound, "请求的对象文件不存在")
			}
			return nil, err
		}
		return f, nil
	}

	p.mu.RLock()
	obj, exists := p.objects[cleanedKey]
	p.mu.RUnlock()
	if !exists {
		return nil, xerr.NewCodeError(xerr.NotFound, "请求的对象文件不存在")
	}
	copied := make([]byte, len(obj.data))
	copy(copied, obj.data)
	return io.NopCloser(bytes.NewReader(copied)), nil
}

// DeleteObject 从内存或本地磁盘删除指定 objectKey 的对象。
func (p *LocalProvider) DeleteObject(ctx context.Context, objectKey string) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	cleanedKey, err := cleanObjectKey(objectKey)
	if err != nil {
		return err
	}

	if p.useFilesystem() {
		fullPath, err := p.resolveLocalFilePath(cleanedKey)
		if err != nil {
			return err
		}
		if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}

	p.mu.Lock()
	delete(p.objects, cleanedKey)
	p.mu.Unlock()
	return nil
}

// PresignedPutURL 生成多租户隔离的对象 Key 以及带有 HMAC-SHA256 签名的本地预签名上传 URL。
func (p *LocalProvider) PresignedPutURL(ctx context.Context, originalFilename string, expires time.Duration) (string, string, error) {
	if ctx != nil && ctx.Err() != nil {
		return "", "", ctx.Err()
	}

	objectKey, err := buildTenantObjectKeyWithExts(ctx, originalFilename, p.cfg.AllowedExts)
	if err != nil {
		return "", "", err
	}
	signedURL, err := p.buildSignedURL("PUT", objectKey, expires)
	if err != nil {
		return "", "", err
	}
	return objectKey, signedURL, nil
}

// PresignedGetURL 为指定 objectKey 生成带有 HMAC-SHA256 签名的本地预签名下载 URL。
func (p *LocalProvider) PresignedGetURL(ctx context.Context, objectKey string, expires time.Duration) (string, error) {
	if ctx != nil && ctx.Err() != nil {
		return "", ctx.Err()
	}

	cleanedKey, err := cleanObjectKey(objectKey)
	if err != nil {
		return "", err
	}
	return p.buildSignedURL("GET", cleanedKey, expires)
}

// buildSignedURL 使用 HMAC-SHA256 构造带有过期时间戳与签名的 URL。
func (p *LocalProvider) buildSignedURL(method string, objectKey string, expires time.Duration) (string, error) {
	if expires <= 0 {
		expires = DefaultPresignExpires
	}
	expUnix := time.Now().UTC().Add(expires).Unix()
	payload := fmt.Sprintf("%s\n%s\n%s\n%d", strings.ToUpper(method), p.cfg.BucketName, objectKey, expUnix)

	mac := hmac.New(sha256.New, []byte(p.cfg.SecretKey))
	_, _ = mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))

	q := url.Values{}
	q.Set("X-Blade-Algorithm", "HMAC-SHA256")
	q.Set("X-Blade-AccessKey", p.cfg.AccessKey)
	q.Set("X-Blade-Expires", strconv.FormatInt(expUnix, 10))
	q.Set("X-Blade-Signature", sig)

	return fmt.Sprintf("%s/%s/%s?%s", p.cfg.Endpoint, p.cfg.BucketName, escapeObjectKeyPath(objectKey), q.Encode()), nil
}
