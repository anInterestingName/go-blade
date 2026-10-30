package jwt

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/golang-jwt/jwt/v5"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

func TestJWT(t *testing.T) {
	handler, err := NewJWTHandler("secret-key-123456", 2*time.Hour, 24*time.Hour, "blade")
	if err != nil {
		t.Fatalf("new jwt handler failed: %v", err)
	}

	// 使用超过 JS MAX_SAFE_INTEGER (9007199254740991) 的雪花 ID 验证精度不丢失
	largeSnowflakeID := int64(9007199254740993)

	payload := TokenPayload{
		TenantID: "000000",
		UserID:   largeSnowflakeID,
		DeptID:   largeSnowflakeID,
		Account:  "admin",
		UserName: "系统管理员",
		RoleName: "administrator",
	}

	res, err := handler.GenerateToken(payload)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	if res.AccessToken == "" || res.RefreshToken == "" {
		t.Fatal("empty access or refresh token")
	}

	// 1. 验证 AccessToken 正常解析及雪花 ID JSON 序列化为 string
	claims, err := handler.ParseToken(res.AccessToken)
	if err != nil {
		t.Fatalf("parse token failed: %v", err)
	}
	if claims.TenantID != "000000" || claims.UserID != largeSnowflakeID || claims.Account != "admin" {
		t.Fatalf("claims content mismatch: %+v", claims)
	}
	if claims.Category != TokenTypeAccess {
		t.Fatalf("expected category access_token, got %s", claims.Category)
	}

	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims failed: %v", err)
	}
	var rawMap map[string]any
	if err := json.Unmarshal(claimsJSON, &rawMap); err != nil {
		t.Fatalf("unmarshal raw claims map failed: %v", err)
	}
	if uidStr, ok := rawMap["user_id"].(string); !ok || uidStr != "9007199254740993" {
		t.Fatalf("expected user_id serialized as string '9007199254740993', got %T: %v", rawMap["user_id"], rawMap["user_id"])
	}

	// 2. 验证拿 RefreshToken 请求 ParseToken 会被拦截
	_, err = handler.ParseToken(res.RefreshToken)
	if err != ErrTokenTypeMismatch {
		t.Fatalf("expected ErrTokenTypeMismatch when passing refresh token to ParseToken, got: %v", err)
	}

	// 3. 验证 ParseRefreshToken 可以成功解析 RefreshToken
	refreshClaims, err := handler.ParseRefreshToken(res.RefreshToken)
	if err != nil {
		t.Fatalf("parse refresh token failed: %v", err)
	}
	if refreshClaims.Account != "admin" || refreshClaims.Category != TokenTypeRefresh {
		t.Fatalf("unexpected refresh claims: %+v", refreshClaims)
	}

	// 4. 验证令牌轮换 RefreshToken（首次轮换成功）
	newRes, err := handler.RefreshToken(res.RefreshToken)
	if err != nil {
		t.Fatalf("refresh token exchange failed: %v", err)
	}
	if newRes.AccessToken == "" || newRes.RefreshToken == "" {
		t.Fatal("new tokens must not be empty")
	}
	newClaims, err := handler.ParseToken(newRes.AccessToken)
	if err != nil {
		t.Fatalf("parse new access token failed: %v", err)
	}
	if newClaims.UserID != largeSnowflakeID {
		t.Fatalf("expected user id %d, got %d", largeSnowflakeID, newClaims.UserID)
	}

	// 5. 验证旧 RefreshToken 重放攻击拦截（已被消费的 RefreshToken 再次调用必须失败）
	_, err = handler.RefreshToken(res.RefreshToken)
	if err != ErrRefreshTokenRevoked {
		t.Fatalf("expected ErrRefreshTokenRevoked on replay of old refresh token, got: %v", err)
	}

	// 6. 验证新签发的 RefreshToken 可以正常继续轮换一次
	secondRes, err := handler.RefreshToken(newRes.RefreshToken)
	if err != nil || secondRes.AccessToken == "" {
		t.Fatalf("expected new refresh token to succeed once, got err: %v", err)
	}

	// 7. 边界校验：密钥为空报错
	_, err = NewJWTHandler("", time.Hour, time.Hour, "")
	if err != ErrEmptyTokenSecret {
		t.Fatalf("expected ErrEmptyTokenSecret, got %v", err)
	}

	// 8. 大小写不敏感 Bearer 前缀解析（如 "BEARER <token>"、"bEaReR <token>"）
	for _, prefix := range []string{"Bearer ", "bearer ", "BEARER ", "bEaReR   "} {
		parsed, pErr := handler.ParseToken(prefix + secondRes.AccessToken)
		if pErr != nil || parsed.UserID != largeSnowflakeID {
			t.Fatalf("expected case-insensitive prefix %q to parse successfully, got err: %v", prefix, pErr)
		}
	}
}

func TestTokenBlacklist_RevokeAndIntercept(t *testing.T) {
	ctx := context.Background()
	handler, err := NewJWTHandler("secret-key-blacklist-test", time.Hour, 24*time.Hour, "blade")
	if err != nil {
		t.Fatalf("new jwt handler failed: %v", err)
	}

	// 1. 内存模式下签发与吊销验证
	pkgredis.SetRedis(nil)
	res, err := handler.GenerateTokenCtx(ctx, TokenPayload{
		TenantID: "000000",
		UserID:   10086,
		Account:  "revoke_user",
		UserName: "测试吊销用户",
	})
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	claims, err := handler.ParseToken(res.AccessToken)
	if err != nil {
		t.Fatalf("expected valid token before revocation, got: %v", err)
	}

	if IsTokenRevoked(ctx, res.AccessToken) {
		t.Fatal("token should not be revoked yet")
	}

	// 使用 RevokeTokenWithClaims 主动吊销 AccessToken
	if err := RevokeTokenWithClaims(ctx, "Bearer "+res.AccessToken, claims); err != nil {
		t.Fatalf("RevokeTokenWithClaims failed: %v", err)
	}

	if !IsTokenRevoked(ctx, res.AccessToken) {
		t.Fatal("expected IsTokenRevoked to return true after revocation")
	}

	// 再次调用 ParseToken 必须返回 ErrTokenRevoked
	if _, err := handler.ParseToken(res.AccessToken); err != ErrTokenRevoked {
		t.Fatalf("expected ErrTokenRevoked on ParseToken, got: %v", err)
	}

	// 吊销 RefreshToken 后再次 RefreshToken 必须返回 ErrTokenRevoked
	if err := RevokeToken(ctx, res.RefreshToken, time.Minute); err != nil {
		t.Fatalf("RevokeToken on refresh token failed: %v", err)
	}
	if _, err := handler.RefreshTokenCtx(ctx, res.RefreshToken); err != ErrTokenRevoked {
		t.Fatalf("expected ErrTokenRevoked on RefreshToken, got: %v", err)
	}

	// 2. Redis + JWKSValidator + CompositeValidator 吊销拦截验证
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis failed: %v", err)
	}
	defer mr.Close()

	rds := zeroredis.MustNewRedis(zeroredis.RedisConf{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	})
	pkgredis.SetRedis(rds)
	defer pkgredis.SetRedis(nil)

	rsaPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key failed: %v", err)
	}
	jwksVal := NewJWKSValidator("", time.Hour)
	jwksVal.SetRSAPublicKey("k1", &rsaPriv.PublicKey)

	rsaClaims := BladeClaims{
		TenantID: "000000",
		UserID:   20086,
		Account:  "jwks_user",
		Category: TokenTypeAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	rsaTok := jwt.NewWithClaims(jwt.SigningMethodRS256, rsaClaims)
	rsaTok.Header["kid"] = "k1"
	rsaTokenStr, err := rsaTok.SignedString(rsaPriv)
	if err != nil {
		t.Fatalf("sign rsa token failed: %v", err)
	}

	comp := NewCompositeValidator(jwksVal, handler)
	if _, err := comp.ValidateToken(ctx, rsaTokenStr); err != nil {
		t.Fatalf("expected valid rsa token before revocation, got: %v", err)
	}

	if err := RevokeToken(ctx, rsaTokenStr, 10*time.Minute); err != nil {
		t.Fatalf("RevokeToken for rsa token failed: %v", err)
	}

	if _, err := jwksVal.ParseTokenWithContext(ctx, rsaTokenStr); err != ErrTokenRevoked {
		t.Fatalf("expected ErrTokenRevoked from JWKSValidator.ParseTokenWithContext, got: %v", err)
	}
	if _, err := comp.ValidateToken(ctx, rsaTokenStr); err != ErrTokenRevoked {
		t.Fatalf("expected ErrTokenRevoked from CompositeValidator.ValidateToken, got: %v", err)
	}
}
