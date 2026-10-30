package jwt

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJWKSValidator_LocalKey(t *testing.T) {
	// 1. 生成 RSA 测试密钥对
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	kid := "test-key-id-1"
	validator := NewJWKSValidator("", time.Hour)
	validator.SetRSAPublicKey(kid, &privKey.PublicKey)

	// 2. 签发带 BladeClaims 与 kid 的 Token
	claims := BladeClaims{
		TenantID: "000000",
		UserID:   18821038102,
		DeptID:   1001,
		RoleName: "admin",
		Account:  "blade_admin",
		UserName: "管理员",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "http://localhost:4444/",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	tokenStr, err := token.SignedString(privKey)
	require.NoError(t, err)

	// 3. 校验与解析
	parsedClaims, err := validator.ParseAndValidate(context.Background(), "Bearer "+tokenStr)
	assert.NoError(t, err)
	assert.Equal(t, "000000", parsedClaims.TenantID)
	assert.Equal(t, int64(18821038102), parsedClaims.UserID)
	assert.Equal(t, "blade_admin", parsedClaims.Account)

	// 4. 过期 Token 测试
	expiredClaims := claims
	expiredClaims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Hour))
	expiredToken := jwt.NewWithClaims(jwt.SigningMethodRS256, expiredClaims)
	expiredToken.Header["kid"] = kid
	expiredTokenStr, err := expiredToken.SignedString(privKey)
	require.NoError(t, err)

	_, err = validator.ParseAndValidate(context.Background(), expiredTokenStr)
	assert.ErrorIs(t, err, ErrTokenExpired)
}

func TestJWKSValidator_RemoteFetch(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	kid := "remote-key-1"
	nStr := base64.RawURLEncoding.EncodeToString(privKey.N.Bytes())
	eBytes := big.NewInt(int64(privKey.E)).Bytes()
	eStr := base64.RawURLEncoding.EncodeToString(eBytes)

	// 模拟 Hydra JWKS 端点
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jwks := JSONWebKeySet{
			Keys: []JSONWebKey{
				{
					Kty: "RSA",
					Kid: kid,
					Use: "sig",
					Alg: "RS256",
					N:   nStr,
					E:   eStr,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer server.Close()

	validator := NewJWKSValidator(server.URL, time.Minute)

	claims := BladeClaims{
		TenantID: "000000",
		UserID:   999,
		Account:  "remote_user",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	tokenStr, err := token.SignedString(privKey)
	require.NoError(t, err)

	parsed, err := validator.ParseAndValidate(context.Background(), tokenStr)
	assert.NoError(t, err)
	assert.Equal(t, "remote_user", parsed.Account)

	// 测试 CompositeValidator 双模回退（JWKS + HMAC）
	hmacHandler, err := NewJWTHandler("composite-secret-key", time.Hour, 24*time.Hour, "blade")
	require.NoError(t, err)
	hmacRes, err := hmacHandler.GenerateToken(TokenPayload{
		TenantID: "000000",
		UserID:   777,
		Account:  "hmac_user",
	})
	require.NoError(t, err)

	composite := NewCompositeValidator(validator, hmacHandler)

	// 1) RSA JWKS Token 通过 CompositeValidator
	c1, err := composite.ValidateToken(context.Background(), tokenStr)
	require.NoError(t, err)
	assert.Equal(t, "remote_user", c1.Account)

	// 2) HMAC Token 自动回退并验签通过
	c2, err := composite.ValidateToken(context.Background(), hmacRes.AccessToken)
	require.NoError(t, err)
	assert.Equal(t, "hmac_user", c2.Account)

	// 3) RefreshToken 必须被拦截返回 ErrTokenTypeMismatch
	_, err = composite.ValidateToken(context.Background(), hmacRes.RefreshToken)
	assert.ErrorIs(t, err, ErrTokenTypeMismatch)
}
