// Package jwt 提供与 SpringBlade / Saber 前端规范兼容的 JWT 解析与签发服务。
// 支持基于 JWKS (JSON Web Key Set) 的公钥验签与 Ory Hydra 令牌解析。
package jwt

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	// ErrJWKSKeyNotFound 未能从 JWKS 中找到对应 kid 的公钥
	ErrJWKSKeyNotFound = errors.New("jwks key not found for given kid")
	// ErrJWKSUnsupportedKeyType 不支持的公钥加密类型（当前支持 RSA）
	ErrJWKSUnsupportedKeyType = errors.New("unsupported jwks key type")
)

// JSONWebKey JWK 键描述
type JSONWebKey struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// JSONWebKeySet JWKS 集合响应
type JSONWebKeySet struct {
	Keys []JSONWebKey `json:"keys"`
}

// JWKSValidator 支持动态拉取或注入 JWKS 公钥并校验 Token 的校验器接口。
type JWKSValidator struct {
	jwksURL    string
	httpClient *http.Client
	mu         sync.RWMutex
	keys       map[string]*rsa.PublicKey
	updatedAt  time.Time
	cacheTTL   time.Duration
}

// NewJWKSValidator 创建并初始化一个 JWKS 公钥验签器。
// jwksURL: Hydra 的公钥端点，例如 "http://localhost:4444/.well-known/jwks.json"。如果为空则需要手动 SetPublicKey 注入。
// cacheTTL: 远程 JWKS 公钥缓存有效期（推荐 1 小时）。
func NewJWKSValidator(jwksURL string, cacheTTL time.Duration) *JWKSValidator {
	if cacheTTL <= 0 {
		cacheTTL = time.Hour
	}
	return &JWKSValidator{
		jwksURL:    jwksURL,
		cacheTTL:   cacheTTL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		keys:       make(map[string]*rsa.PublicKey),
	}
}

// SetRSAPublicKey 手动设置某个 kid 对应的 RSA 公钥（通常用于单元测试或预置静态公钥）。
func (v *JWKSValidator) SetRSAPublicKey(kid string, pubKey *rsa.PublicKey) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.keys[kid] = pubKey
}

// RefreshKeys 强制刷新 JWKS 公钥集合。
func (v *JWKSValidator) RefreshKeys(ctx context.Context) error {
	if v.jwksURL == "" {
		return errors.New("jwks url is empty")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return err
	}

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch jwks: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch jwks returned non-200 code: %d", resp.StatusCode)
	}

	var jwks JSONWebKeySet
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return fmt.Errorf("failed to decode jwks json: %w", err)
	}

	newKeys := make(map[string]*rsa.PublicKey)
	for _, k := range jwks.Keys {
		if k.Kty == "RSA" && k.N != "" && k.E != "" {
			pub, err := parseRSAPublicKey(k.N, k.E)
			if err == nil {
				newKeys[k.Kid] = pub
			}
		}
	}

	v.mu.Lock()
	v.keys = newKeys
	v.updatedAt = time.Now()
	v.mu.Unlock()

	return nil
}

// GetKey 根据 kid 获取对应的 RSA 公钥，必要时自动拉取刷新。
func (v *JWKSValidator) GetKey(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.RLock()
	key, exists := v.keys[kid]
	expired := time.Since(v.updatedAt) > v.cacheTTL
	v.mu.RUnlock()

	if exists && !expired {
		return key, nil
	}

	// 尝试刷新
	if v.jwksURL != "" {
		if err := v.RefreshKeys(ctx); err != nil && !exists {
			return nil, err
		}
	}

	v.mu.RLock()
	defer v.mu.RUnlock()
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}

	return nil, ErrJWKSKeyNotFound
}

// ParseAndValidate 解析基于 JWKS 签名的 JWT 并校验签名，返回 BladeClaims。
func (v *JWKSValidator) ParseAndValidate(ctx context.Context, tokenStr string) (*BladeClaims, error) {
	tokenStr = normalizeTokenString(tokenStr)

	if tokenStr == "" {
		return nil, ErrTokenInvalid
	}

	if IsTokenRevoked(ctx, tokenStr) {
		return nil, ErrTokenRevoked
	}

	token, err := jwt.ParseWithClaims(tokenStr, &BladeClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}

		kid, ok := t.Header["kid"].(string)
		if !ok || kid == "" {
			// 如果没有 kid 且只有一个公钥，则尝试使用唯一的公钥
			v.mu.RLock()
			defer v.mu.RUnlock()
			if len(v.keys) == 1 {
				for _, k := range v.keys {
					return k, nil
				}
			}
			return nil, ErrJWKSKeyNotFound
		}

		return v.GetKey(ctx, kid)
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, ErrTokenInvalid
	}

	if claims, ok := token.Claims.(*BladeClaims); ok && token.Valid {
		if claims.Category == TokenTypeRefresh {
			return nil, ErrTokenTypeMismatch
		}
		if claims.Category == "" {
			claims.Category = TokenTypeAccess
		}
		return claims, nil
	}

	return nil, ErrTokenInvalid
}

// ParseToken 使用 JWKS 公钥校验并解析客户端传入的访问令牌。
func (v *JWKSValidator) ParseToken(tokenStr string) (*BladeClaims, error) {
	return v.ParseAndValidate(context.Background(), tokenStr)
}

// ParseTokenWithContext 携带上下文使用 JWKS 公钥校验并解析客户端传入的访问令牌。
func (v *JWKSValidator) ParseTokenWithContext(ctx context.Context, tokenStr string) (*BladeClaims, error) {
	return v.ParseAndValidate(ctx, tokenStr)
}

// ValidateToken 实现 TokenValidator 接口，使用 JWKS 公钥校验并解析访问令牌。
func (v *JWKSValidator) ValidateToken(ctx context.Context, tokenStr string) (*BladeClaims, error) {
	return v.ParseAndValidate(ctx, tokenStr)
}

// parseRSAPublicKey 将 JWK 的 N 和 E 模数与指数转换成 rsa.PublicKey
func parseRSAPublicKey(nStr, eStr string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nStr)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eStr)
	if err != nil {
		return nil, err
	}

	n := new(big.Int).SetBytes(nBytes)
	var eInt int
	for _, b := range eBytes {
		eInt = (eInt << 8) | int(b)
	}

	return &rsa.PublicKey{
		N: n,
		E: eInt,
	}, nil
}
