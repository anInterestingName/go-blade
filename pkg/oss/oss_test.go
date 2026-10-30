// Package oss 提供多租户隔离的统一对象存储抽象层单元测试。
package oss

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
)

func TestBuildTenantObjectKey_TenantIsolationAndSecurity(t *testing.T) {
	// 1. 默认租户 (000000)
	keyDefault, err := BuildTenantObjectKey(context.Background(), "avatar.png")
	if err != nil {
		t.Fatalf("expected nil error for valid png, got: %v", err)
	}
	if !strings.HasPrefix(keyDefault, "000000/") {
		t.Fatalf("expected default tenant prefix '000000/', got: %s", keyDefault)
	}
	if !strings.HasSuffix(keyDefault, "_avatar.png") {
		t.Fatalf("expected suffix '_avatar.png', got: %s", keyDefault)
	}

	// 2. 指定租户隔离 (888888)
	ctxTenant := tenant.WithTenant(context.Background(), "888888")
	keyTenant, err := BuildTenantObjectKey(ctxTenant, "report.pdf")
	if err != nil {
		t.Fatalf("expected nil error for valid pdf, got: %v", err)
	}
	if !strings.HasPrefix(keyTenant, "888888/") {
		t.Fatalf("expected tenant prefix '888888/', got: %s", keyTenant)
	}
	if !strings.HasSuffix(keyTenant, "_report.pdf") {
		t.Fatalf("expected suffix '_report.pdf', got: %s", keyTenant)
	}

	// 3. 路径穿越攻击防护 (Unix 与 Windows 风格路径)
	traversalInputs := []string{
		"../../../etc/passwd.txt",
		"..\\..\\windows\\win.txt",
		"/var/www/uploads/../../secret.jpg",
	}
	for _, input := range traversalInputs {
		key, err := BuildTenantObjectKey(ctxTenant, input)
		if err != nil {
			t.Fatalf("expected sanitized filename for %q, got error: %v", input, err)
		}
		if strings.Contains(key, "..") {
			t.Fatalf("object key %q still contains path traversal segment for input %q", key, input)
		}
		parts := strings.Split(key, "/")
		if len(parts) != 3 {
			t.Fatalf("expected exactly 3 path segments <tenant>/<date>/<id_file>, got %d in %q", len(parts), key)
		}
	}

	// 4. 非法扩展名拦截 (.exe, .sh, .php, .jsp, 无扩展名)
	forbiddenFiles := []string{
		"malware.exe",
		"shell.sh",
		"backdoor.php",
		"hack.jsp",
		"no_extension_file",
		"../..",
		"",
	}
	for _, badFile := range forbiddenFiles {
		_, err := BuildTenantObjectKey(ctxTenant, badFile)
		if err == nil {
			t.Fatalf("expected error for forbidden filename %q, got nil", badFile)
		}
		ce, ok := err.(*xerr.CodeError)
		if !ok || ce.Code != xerr.RequestParamError {
			t.Fatalf("expected RequestParamError(400) for %q, got: %v", badFile, err)
		}
	}
}

func TestLocalProvider_MemoryAndFilesystem(t *testing.T) {
	ctx := tenant.WithTenant(context.Background(), "100001")

	t.Run("MemoryDriver", func(t *testing.T) {
		provider, err := NewProvider(Config{
			Driver:      DriverMemory,
			Endpoint:    "http://127.0.0.1:9000",
			BucketName:  "blade-mem",
			MaxFileSize: 1024, // 1KB 限制
		})
		if err != nil {
			t.Fatalf("NewProvider failed: %v", err)
		}

		payload := []byte("hello blade memory oss")
		ossFile, err := provider.PutObject(ctx, "hello.txt", bytes.NewReader(payload), int64(len(payload)), "text/plain")
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
		if !strings.HasPrefix(ossFile.Name, "100001/") || !strings.HasSuffix(ossFile.Name, "_hello.txt") {
			t.Fatalf("unexpected object key: %s", ossFile.Name)
		}
		if ossFile.Size != int64(len(payload)) {
			t.Fatalf("expected size %d, got %d", len(payload), ossFile.Size)
		}

		// GetObject
		rc, err := provider.GetObject(ctx, ossFile.Name)
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		gotBytes, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil || !bytes.Equal(gotBytes, payload) {
			t.Fatalf("GetObject content mismatch, got %q, want %q", string(gotBytes), string(payload))
		}

		// PresignedPutURL & PresignedGetURL
		putKey, putURL, err := provider.PresignedPutURL(ctx, "photo.jpg", 10*time.Minute)
		if err != nil {
			t.Fatalf("PresignedPutURL failed: %v", err)
		}
		if !strings.HasPrefix(putKey, "100001/") || !strings.Contains(putURL, "X-Blade-Signature=") {
			t.Fatalf("unexpected presigned put result: key=%s, url=%s", putKey, putURL)
		}

		getURL, err := provider.PresignedGetURL(ctx, ossFile.Name, 10*time.Minute)
		if err != nil || !strings.Contains(getURL, "X-Blade-Signature=") {
			t.Fatalf("PresignedGetURL failed: url=%s, err=%v", getURL, err)
		}

		// 超大文件拦截（声明大小超限 & 实际流大小超限）
		oversize := bytes.Repeat([]byte("a"), 2048)
		if _, err := provider.PutObject(ctx, "big.txt", bytes.NewReader(oversize), int64(len(oversize)), "text/plain"); err == nil {
			t.Fatalf("expected error for oversized file, got nil")
		}
		// 伪造 size=10 但实际发送 2048 字节
		if _, err := provider.PutObject(ctx, "spoof.txt", bytes.NewReader(oversize), 10, "text/plain"); err == nil {
			t.Fatalf("expected error when stream exceeds MaxFileSize despite small declared size")
		}

		// DeleteObject
		if err := provider.DeleteObject(ctx, ossFile.Name); err != nil {
			t.Fatalf("DeleteObject failed: %v", err)
		}
		if _, err := provider.GetObject(ctx, ossFile.Name); err == nil {
			t.Fatalf("expected NotFound error after DeleteObject, got nil")
		}
	})

	t.Run("FilesystemDriver", func(t *testing.T) {
		tempDir := t.TempDir()
		provider, err := NewProvider(Config{
			Driver:     DriverLocal,
			BaseDir:    tempDir,
			Endpoint:   "http://localhost:8080",
			BucketName: "blade-local",
		})
		if err != nil {
			t.Fatalf("NewProvider local failed: %v", err)
		}

		content := []byte("filesystem oss content")
		ossFile, err := provider.PutObject(ctx, "doc.pdf", bytes.NewReader(content), int64(len(content)), "application/pdf")
		if err != nil {
			t.Fatalf("PutObject local failed: %v", err)
		}

		rc, err := provider.GetObject(ctx, ossFile.Name)
		if err != nil {
			t.Fatalf("GetObject local failed: %v", err)
		}
		readBack, _ := io.ReadAll(rc)
		_ = rc.Close()
		if !bytes.Equal(readBack, content) {
			t.Fatalf("local file content mismatch")
		}

		// 校验 GetObject 路径穿越拦截
		if _, err := provider.GetObject(ctx, "../../etc/passwd"); err == nil {
			t.Fatalf("expected path traversal rejection in GetObject")
		}

		if err := provider.DeleteObject(ctx, ossFile.Name); err != nil {
			t.Fatalf("DeleteObject local failed: %v", err)
		}
	})
}

func TestS3Provider_WithMockS3Server(t *testing.T) {
	var (
		mu      sync.Mutex
		storage = make(map[string][]byte)
	)

	// 构建模拟 MinIO / S3 服务端，校验 AWS Signature V4 请求头与方法
	mockS3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		amzDate := r.Header.Get("X-Amz-Date")
		contentSha := r.Header.Get("X-Amz-Content-Sha256")

		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=test-ak/") ||
			!strings.Contains(auth, "SignedHeaders=") ||
			!strings.Contains(auth, "Signature=") ||
			amzDate == "" || contentSha == "" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("Invalid AWS4 signature headers"))
			return
		}

		mu.Lock()
		defer mu.Unlock()

		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			// 校验负载 SHA256 与 X-Amz-Content-Sha256 完全一致
			if sha256Hex(body) != contentSha {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("X-Amz-Content-Sha256 mismatch"))
				return
			}
			storage[r.URL.Path] = body
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			data, ok := storage[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "image/png")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)

		case http.MethodDelete:
			delete(storage, r.URL.Path)
			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer mockS3.Close()

	provider, err := NewProvider(Config{
		Driver:     DriverMinIO,
		Endpoint:   mockS3.URL,
		AccessKey:  "test-ak",
		SecretKey:  "test-sk",
		BucketName: "blade-bucket",
		Region:     "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewProvider S3 failed: %v", err)
	}

	ctx := tenant.WithTenant(context.Background(), "200002")
	imgData := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}

	// 1. PutObject
	ossFile, err := provider.PutObject(ctx, "logo.png", bytes.NewReader(imgData), int64(len(imgData)), "image/png")
	if err != nil {
		t.Fatalf("S3 PutObject failed: %v", err)
	}
	if !strings.HasPrefix(ossFile.Name, "200002/") || !strings.HasSuffix(ossFile.Name, "_logo.png") {
		t.Fatalf("unexpected S3 object key: %s", ossFile.Name)
	}
	if ossFile.Domain != mockS3.URL {
		t.Fatalf("expected domain %s, got %s", mockS3.URL, ossFile.Domain)
	}

	// 2. GetObject
	rc, err := provider.GetObject(ctx, ossFile.Name)
	if err != nil {
		t.Fatalf("S3 GetObject failed: %v", err)
	}
	downloaded, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(downloaded, imgData) {
		t.Fatalf("S3 GetObject data mismatch")
	}

	// 3. PresignedPutURL & PresignedGetURL (校验 AWS4-HMAC-SHA256 查询参数)
	putKey, putURL, err := provider.PresignedPutURL(ctx, "banner.webp", 15*time.Minute)
	if err != nil {
		t.Fatalf("S3 PresignedPutURL failed: %v", err)
	}
	if !strings.HasPrefix(putKey, "200002/") {
		t.Fatalf("expected tenant prefix in presigned putKey, got %s", putKey)
	}
	parsedPut, err := url.Parse(putURL)
	if err != nil {
		t.Fatalf("failed to parse presigned put URL: %v", err)
	}
	q := parsedPut.Query()
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" ||
		!strings.HasPrefix(q.Get("X-Amz-Credential"), "test-ak/") ||
		q.Get("X-Amz-Expires") != "900" ||
		q.Get("X-Amz-SignedHeaders") != "host" ||
		len(q.Get("X-Amz-Signature")) != 64 {
		t.Fatalf("invalid S3 presigned PUT query params: %s", putURL)
	}

	getURL, err := provider.PresignedGetURL(ctx, ossFile.Name, 5*time.Minute)
	if err != nil {
		t.Fatalf("S3 PresignedGetURL failed: %v", err)
	}
	parsedGet, err := url.Parse(getURL)
	if err != nil || parsedGet.Query().Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		t.Fatalf("invalid S3 presigned GET URL: %s", getURL)
	}

	// 4. DeleteObject & 验证 404
	if err := provider.DeleteObject(ctx, ossFile.Name); err != nil {
		t.Fatalf("S3 DeleteObject failed: %v", err)
	}
	_, err = provider.GetObject(ctx, ossFile.Name)
	if err == nil {
		t.Fatalf("expected NotFound error after S3 DeleteObject, got nil")
	}
	if ce, ok := err.(*xerr.CodeError); !ok || ce.Code != xerr.NotFound {
		t.Fatalf("expected xerr.NotFound(404), got %v", err)
	}
}

func TestUploadAndPresignedHTTPHandlers(t *testing.T) {
	provider, err := NewProvider(Config{
		Driver:      DriverMemory,
		Endpoint:    "http://oss.blade.local",
		BucketName:  "blade-resource",
		MaxFileSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	uploadHandler := UploadHTTPHandler(provider)
	presignHandler := PresignedURLHTTPHandler(provider)

	// 1. 测试表单上传成功（携带 Blade-Tenant-Id 请求头）
	var bodyBuf bytes.Buffer
	mw := multipart.NewWriter(&bodyBuf)
	fw, err := mw.CreateFormFile("file", "../../avatar.png")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	fileBytes := []byte("fake-png-binary-content")
	_, _ = fw.Write(fileBytes)
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/blade-resource/oss/endpoint/put-file", &bodyBuf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set(tenant.HeaderTenantID, "999999")
	rec := httptest.NewRecorder()

	uploadHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var uploadResp response.Response[OssFile]
	if err := json.Unmarshal(rec.Body.Bytes(), &uploadResp); err != nil {
		t.Fatalf("failed to unmarshal upload response: %v", err)
	}
	if !uploadResp.Success || uploadResp.Code != xerr.OK {
		t.Fatalf("expected success response, got: %+v", uploadResp)
	}
	if !strings.HasPrefix(uploadResp.Data.Name, "999999/") || !strings.HasSuffix(uploadResp.Data.Name, "_avatar.png") {
		t.Fatalf("expected sanitized tenant object key, got: %s", uploadResp.Data.Name)
	}
	if strings.Contains(uploadResp.Data.Name, "..") {
		t.Fatalf("path traversal not stripped in uploaded object key: %s", uploadResp.Data.Name)
	}
	if uploadResp.Data.Size != int64(len(fileBytes)) {
		t.Fatalf("expected size %d, got %d", len(fileBytes), uploadResp.Data.Size)
	}

	// 2. 测试上传非法后缀文件被拦截
	var badBuf bytes.Buffer
	badMw := multipart.NewWriter(&badBuf)
	badFw, _ := badMw.CreateFormFile("file", "trojan.exe")
	_, _ = badFw.Write([]byte("MZ..."))
	_ = badMw.Close()

	badReq := httptest.NewRequest(http.MethodPost, "/blade-resource/oss/endpoint/put-file", &badBuf)
	badReq.Header.Set("Content-Type", badMw.FormDataContentType())
	badRec := httptest.NewRecorder()

	uploadHandler.ServeHTTP(badRec, badReq)

	var badResp response.Response[any]
	if err := json.Unmarshal(badRec.Body.Bytes(), &badResp); err != nil {
		t.Fatalf("failed to unmarshal bad upload response: %v", err)
	}
	if badResp.Success || badResp.Code != xerr.RequestParamError {
		t.Fatalf("expected 400 RequestParamError for .exe upload, got: %+v", badResp)
	}

	// 3. 测试 PresignedURLHTTPHandler (PUT & GET)
	putPresignReq := httptest.NewRequest(http.MethodGet, "/blade-resource/oss/endpoint/presigned-url?method=PUT&filename=contract.pdf&expires=600", nil)
	putPresignReq.Header.Set(tenant.HeaderTenantID, "999999")
	putPresignRec := httptest.NewRecorder()
	presignHandler.ServeHTTP(putPresignRec, putPresignReq)

	var putPresignResp response.Response[PresignedURLData]
	if err := json.Unmarshal(putPresignRec.Body.Bytes(), &putPresignResp); err != nil {
		t.Fatalf("failed to unmarshal presigned PUT response: %v", err)
	}
	if !putPresignResp.Success || !strings.HasPrefix(putPresignResp.Data.ObjectKey, "999999/") || putPresignResp.Data.Expires != 600 {
		t.Fatalf("unexpected presigned PUT response: %+v", putPresignResp)
	}

	getPresignReq := httptest.NewRequest(http.MethodGet, "/blade-resource/oss/endpoint/presigned-url?method=GET&objectKey="+url.QueryEscape(uploadResp.Data.Name), nil)
	getPresignRec := httptest.NewRecorder()
	presignHandler.ServeHTTP(getPresignRec, getPresignReq)

	var getPresignResp response.Response[PresignedURLData]
	if err := json.Unmarshal(getPresignRec.Body.Bytes(), &getPresignResp); err != nil {
		t.Fatalf("failed to unmarshal presigned GET response: %v", err)
	}
	if !getPresignResp.Success || getPresignResp.Data.Method != http.MethodGet || getPresignResp.Data.URL == "" {
		t.Fatalf("unexpected presigned GET response: %+v", getPresignResp)
	}
}

func FuzzBuildTenantObjectKey_NoPathTraversal(f *testing.F) {
	f.Add("000000", "avatar.png")
	f.Add("../../etc", "../../../passwd.txt")
	f.Add("100001\x00evil", "..\\..\\win\\system.pdf")
	f.Add("   ", "bad:*?\"<>|.jpg")

	f.Fuzz(func(t *testing.T, tenantID string, filename string) {
		ctx := tenant.WithTenant(context.Background(), tenantID)
		key, err := BuildTenantObjectKey(ctx, filename)
		if err == nil {
			if strings.Contains(key, "..") || strings.Contains(key, "\\") || strings.ContainsRune(key, '\x00') {
				t.Fatalf("unsafe object key generated: %q for tenant=%q file=%q", key, tenantID, filename)
			}
			parts := strings.Split(key, "/")
			if len(parts) != 3 {
				t.Fatalf("expected 3 segments in object key %q, got %d", key, len(parts))
			}
		}
	})
}
