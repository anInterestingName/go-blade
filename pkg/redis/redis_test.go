package redis

import (
	"bytes"
	"context"
	"encoding/base64"
	"image/png"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

// setupTestRedis 启动并注册一个 miniredis 内存 Redis 实例用于单测。
func setupTestRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := zeroredis.MustNewRedis(zeroredis.RedisConf{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	})

	SetRedis(client)

	t.Cleanup(func() {
		mr.Close()
		SetRedis(nil)
	})

	return mr
}

func TestConfig_ApplyDefaults(t *testing.T) {
	cfg := Config{}
	cfg.applyDefaults()

	if cfg.Host != "127.0.0.1:6379" {
		t.Errorf("expected default Host 127.0.0.1:6379, got %s", cfg.Host)
	}
	if cfg.Type != zeroredis.NodeType {
		t.Errorf("expected default Type %s, got %s", zeroredis.NodeType, cfg.Type)
	}
	if cfg.PingTimeout != time.Second {
		t.Errorf("expected default PingTimeout 1s, got %v", cfg.PingTimeout)
	}
}

func TestInitRedis_And_GetRedis(t *testing.T) {
	// 重置全局 Redis
	SetRedis(nil)

	// 未初始化时 GetRedis 应当报错
	_, err := GetRedis()
	if err == nil {
		t.Fatal("expected error when redis is not initialized, got nil")
	}

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	// 初始化 Redis
	client, err := InitRedis(Config{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	})
	if err != nil {
		t.Fatalf("InitRedis failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil redis client")
	}

	// 再次获取
	got, err := GetRedis()
	if err != nil {
		t.Fatalf("GetRedis failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil redis client from GetRedis")
	}
}

func TestGenerateAndVerifyCaptcha(t *testing.T) {
	mr := setupTestRedis(t)
	ctx := context.Background()

	// 1. 生成验证码并校验真实 PNG 图形输出
	res, err := GenerateCaptcha(ctx, 5*time.Minute)
	if err != nil {
		t.Fatalf("GenerateCaptcha failed: %v", err)
	}
	if res.Key == "" {
		t.Error("expected non-empty captcha key")
	}
	if len(res.Code) != 4 {
		t.Errorf("expected default captcha code length 4, got %d (%s)", len(res.Code), res.Code)
	}
	if !strings.HasPrefix(res.Image, "data:image/png;base64,") {
		t.Fatalf("expected base64 png data url, got %s", res.Image)
	}
	rawPng, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(res.Image, "data:image/png;base64,"))
	if err != nil {
		t.Fatalf("decode captcha base64 failed: %v", err)
	}
	cfgImg, err := png.DecodeConfig(bytes.NewReader(rawPng))
	if err != nil {
		t.Fatalf("decode png config failed: %v", err)
	}
	if cfgImg.Width <= 1 || cfgImg.Height <= 1 {
		t.Fatalf("expected real captcha image dimensions > 1x1, got %dx%d", cfgImg.Width, cfgImg.Height)
	}

	// 2. 校验失败情况：验证码不一致
	ok, err := VerifyCaptcha(ctx, res.Key, "wrong_code", true)
	if err != nil {
		t.Fatalf("VerifyCaptcha with wrong code returned error: %v", err)
	}
	if ok {
		t.Error("expected verification to fail for wrong code, got true")
	}

	// 校验失败时不应当删除原始验证码
	val, err := mr.Get(CaptchaKeyPrefix + res.Key)
	if err != nil || val != res.Code {
		t.Errorf("expected key to still exist with code %s, got %s (err: %v)", res.Code, val, err)
	}

	// 3. 校验失败情况：key 不存在
	ok, err = VerifyCaptcha(ctx, "non_existent_key", res.Code, true)
	if err != nil {
		t.Fatalf("VerifyCaptcha with non-existent key returned error: %v", err)
	}
	if ok {
		t.Error("expected verification to fail for non existent key, got true")
	}

	// 4. 空参数校验
	ok, err = VerifyCaptcha(ctx, "", res.Code, true)
	if err != nil || ok {
		t.Errorf("expected false, nil for empty key, got %v, %v", ok, err)
	}
	ok, err = VerifyCaptcha(ctx, res.Key, "", true)
	if err != nil || ok {
		t.Errorf("expected false, nil for empty code, got %v, %v", ok, err)
	}

	// 5. 校验成功情况（且 clear = true）
	ok, err = VerifyCaptcha(ctx, res.Key, res.Code, true)
	if err != nil {
		t.Fatalf("VerifyCaptcha with correct code failed: %v", err)
	}
	if !ok {
		t.Error("expected verification to succeed for correct code")
	}

	// 6. 防重放校验：第二次校验相同验证码必须失败
	ok, err = VerifyCaptcha(ctx, res.Key, res.Code, true)
	if err != nil {
		t.Fatalf("Replay VerifyCaptcha returned error: %v", err)
	}
	if ok {
		t.Error("expected replay verification to fail because captcha should be cleared")
	}
}

func TestGenerateCaptchaWithLength(t *testing.T) {
	_ = setupTestRedis(t)
	ctx := context.Background()

	// 长度有效范围：4~6
	lengths := []int{4, 5, 6}
	for _, l := range lengths {
		res, err := GenerateCaptchaWithLength(ctx, l, time.Minute)
		if err != nil {
			t.Fatalf("GenerateCaptchaWithLength(%d) failed: %v", l, err)
		}
		if len(res.Code) != l {
			t.Errorf("expected code length %d, got %d (%s)", l, len(res.Code), res.Code)
		}

		// 校验成功但不删除
		ok, err := VerifyCaptcha(ctx, res.Key, res.Code, false)
		if err != nil || !ok {
			t.Fatalf("VerifyCaptcha with clear=false failed: %v, %v", ok, err)
		}

		// 第二次校验仍然有效
		ok, err = VerifyCaptcha(ctx, res.Key, res.Code, true)
		if err != nil || !ok {
			t.Fatalf("second VerifyCaptcha failed: %v, %v", ok, err)
		}
	}

	// 非法长度测试
	invalidLengths := []int{0, 3, 7, 10}
	for _, l := range invalidLengths {
		_, err := GenerateCaptchaWithLength(ctx, l, time.Minute)
		if err != ErrInvalidCaptchaLength {
			t.Errorf("expected ErrInvalidCaptchaLength for length %d, got %v", l, err)
		}
	}
}

func TestVerifyCaptcha_Expiration(t *testing.T) {
	mr := setupTestRedis(t)
	ctx := context.Background()

	res, err := GenerateCaptcha(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("GenerateCaptcha failed: %v", err)
	}

	// 快进 miniredis 时间
	mr.FastForward(3 * time.Second)

	ok, err := VerifyCaptcha(ctx, res.Key, res.Code, true)
	if err != nil {
		t.Fatalf("VerifyCaptcha returned error on expired key: %v", err)
	}
	if ok {
		t.Error("expected verification to fail for expired captcha")
	}
}
