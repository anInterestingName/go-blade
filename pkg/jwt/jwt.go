// Package jwt 提供与 SpringBlade / Saber 前端规范兼容的 JWT 解析与签发服务。
// 载荷数据结构对齐 BladeUser，支持双 Token（Access + Refresh）轮换与独立类型鉴权。
package jwt

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/snowflake"
	"github.com/golang-jwt/jwt/v5"
)

var (
	// ErrTokenExpired 代表当前 Token 已经超过有效期
	ErrTokenExpired = errors.New("token has expired")
	// ErrTokenInvalid 代表 Token 签名校验失败或内容非法
	ErrTokenInvalid = errors.New("token is invalid")
	// ErrTokenMalformed 代表 Token 格式非标准 JWT 结构
	ErrTokenMalformed = errors.New("token format is malformed")
	// ErrEmptyTokenSecret 代表初始化 JWT 处理器时密钥为空
	ErrEmptyTokenSecret = errors.New("token secret cannot be empty")
	// ErrTokenTypeMismatch 代表当前令牌类别不匹配（例如用 RefreshToken 访问受限 API）
	ErrTokenTypeMismatch = errors.New("token type mismatch")
	// ErrRefreshTokenRevoked 代表刷新令牌已被消费轮换或已撤销（防止重放）
	ErrRefreshTokenRevoked = errors.New("refresh token has been revoked or already used")
	// ErrTokenRevoked 代表当前令牌已被主动登出或加入吊销黑名单
	ErrTokenRevoked = errors.New("token has been revoked")
)

const (
	// TokenTypeAccess 访问令牌标识
	TokenTypeAccess = "access_token"
	// TokenTypeRefresh 刷新令牌标识
	TokenTypeRefresh = "refresh_token"
	// RefreshTokenKeyPrefix 刷新令牌状态在 Redis 中的存储前缀
	RefreshTokenKeyPrefix = "blade:refresh_token:"
	// BlacklistKeyPrefix 主动吊销的令牌黑名单在 Redis 中的存储前缀
	BlacklistKeyPrefix = "blade:jwt:blacklist:"
	// DefaultRevokeTTL 当无法提取有效剩余过期时间时默认使用的黑名单保留时长
	DefaultRevokeTTL = 2 * time.Hour
)

var (
	blacklistMu  sync.RWMutex
	memBlacklist = make(map[string]time.Time)
)

func normalizeTokenString(tokenString string) string {
	tokenString = strings.TrimSpace(tokenString)
	if len(tokenString) >= 7 && strings.EqualFold(tokenString[:7], "Bearer ") {
		tokenString = strings.TrimSpace(tokenString[7:])
	}
	return tokenString
}

func tokenBlacklistKey(tokenString string) string {
	raw := normalizeTokenString(tokenString)
	if raw == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(raw))
	return BlacklistKeyPrefix + hex.EncodeToString(sum[:])
}

// RevokeToken 将指定的 JWT 令牌字符串主动加入吊销黑名单（使用 SHA-256 摘要作为键，同时写入 Redis 与进程内内存字典兜底）。
func RevokeToken(ctx context.Context, tokenString string, ttl time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	key := tokenBlacklistKey(tokenString)
	if key == "" {
		return ErrTokenInvalid
	}
	if ttl <= 0 {
		ttl = DefaultRevokeTTL
	}

	blacklistMu.Lock()
	memBlacklist[key] = time.Now().Add(ttl)
	blacklistMu.Unlock()

	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		sec := int(ttl.Seconds())
		if sec <= 0 {
			sec = 1
		}
		if rErr := rds.SetexCtx(ctx, key, "1", sec); rErr != nil {
			return rErr
		}
	}
	return nil
}

// RevokeTokenWithClaims 根据解析出的 BladeClaims.ExpiresAt 自动计算剩余有效期并调用 RevokeToken 加入黑名单。
func RevokeTokenWithClaims(ctx context.Context, tokenString string, claims *BladeClaims) error {
	ttl := DefaultRevokeTTL
	if claims != nil && claims.ExpiresAt != nil {
		rem := time.Until(claims.ExpiresAt.Time)
		if rem > 0 {
			ttl = rem
		}
	}
	return RevokeToken(ctx, tokenString, ttl)
}

// IsTokenRevoked 检查指定的 JWT 令牌是否已被主动吊销（优先检查 Redis 黑名单，同时检查进程内过期字典兜底）。
func IsTokenRevoked(ctx context.Context, tokenString string) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	key := tokenBlacklistKey(tokenString)
	if key == "" {
		return false
	}

	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		val, rErr := rds.GetCtx(ctx, key)
		if rErr == nil && val != "" {
			return true
		}
	}

	now := time.Now()
	blacklistMu.RLock()
	exp, exists := memBlacklist[key]
	blacklistMu.RUnlock()
	if !exists {
		return false
	}
	if !now.Before(exp) {
		blacklistMu.Lock()
		if curExp, ok := memBlacklist[key]; ok && !now.Before(curExp) {
			delete(memBlacklist, key)
		}
		blacklistMu.Unlock()
		return false
	}
	return true
}

// consumeRefreshLuaScript 原子校验并单次消费 RefreshToken JTI，防止并发重放换票。
const consumeRefreshLuaScript = `
local val = redis.call('GET', KEYS[1])
if not val then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
`

// BladeClaims 对标 SpringBlade 的 BladeUser 声明载荷实体。
// 涵盖租户标识、用户主键、账号名、昵称、角色及部门等鉴权核心元数据。
type BladeClaims struct {
	// TenantID 当前用户所属租户编号
	TenantID string `json:"tenant_id"`
	// UserID 系统用户唯一主键
	UserID int64 `json:"user_id"`
	// DeptID 所属部门主键
	DeptID int64 `json:"dept_id,omitempty"`
	// RoleID 拥有角色主键（多个以逗号拼接）
	RoleID string `json:"role_id,omitempty"`
	// RoleName 拥有角色名称
	RoleName string `json:"role_name,omitempty"`
	// Account 登录账号
	Account string `json:"account"`
	// UserName 用户真实姓名
	UserName string `json:"user_name"`
	// NickName 用户昵称
	NickName string `json:"nick_name,omitempty"`
	// Category 令牌类型区分 ("access_token" | "refresh_token")
	Category string `json:"category,omitempty"`
	// RegisteredClaims JWT 官方标准注册字段 (exp, iat, iss, jti 等)
	jwt.RegisteredClaims
}

type bladeClaimsAlias struct {
	TenantID  string           `json:"tenant_id"`
	UserID    snowflake.ID     `json:"user_id"`
	DeptID    snowflake.ID     `json:"dept_id,omitempty"`
	RoleID    string           `json:"role_id,omitempty"`
	RoleName  string           `json:"role_name,omitempty"`
	Account   string           `json:"account"`
	UserName  string           `json:"user_name"`
	NickName  string           `json:"nick_name,omitempty"`
	Category  string           `json:"category,omitempty"`
	Issuer    string           `json:"iss,omitempty"`
	Subject   string           `json:"sub,omitempty"`
	Audience  jwt.ClaimStrings `json:"aud,omitempty"`
	ExpiresAt *jwt.NumericDate `json:"exp,omitempty"`
	NotBefore *jwt.NumericDate `json:"nbf,omitempty"`
	IssuedAt  *jwt.NumericDate `json:"iat,omitempty"`
	ID        string           `json:"jti,omitempty"`
}

// MarshalJSON 自定义 JSON 序列化，确保 UserID 与 DeptID 在 HTTP 输出或 JWT 载荷中编码为字符串，防止前端 JS 精度丢失。
func (c BladeClaims) MarshalJSON() ([]byte, error) {
	alias := bladeClaimsAlias{
		TenantID:  c.TenantID,
		UserID:    snowflake.ID(c.UserID),
		DeptID:    snowflake.ID(c.DeptID),
		RoleID:    c.RoleID,
		RoleName:  c.RoleName,
		Account:   c.Account,
		UserName:  c.UserName,
		NickName:  c.NickName,
		Category:  c.Category,
		Issuer:    c.Issuer,
		Subject:   c.Subject,
		Audience:  c.Audience,
		ExpiresAt: c.ExpiresAt,
		NotBefore: c.NotBefore,
		IssuedAt:  c.IssuedAt,
		ID:        c.ID,
	}
	return json.Marshal(alias)
}

// UnmarshalJSON 自定义 JSON 反序列化，无缝兼容字符串或数值格式的 UserID 与 DeptID。
func (c *BladeClaims) UnmarshalJSON(b []byte) error {
	var alias bladeClaimsAlias
	if err := json.Unmarshal(b, &alias); err != nil {
		return err
	}
	c.TenantID = alias.TenantID
	c.UserID = alias.UserID.Int64()
	c.DeptID = alias.DeptID.Int64()
	c.RoleID = alias.RoleID
	c.RoleName = alias.RoleName
	c.Account = alias.Account
	c.UserName = alias.UserName
	c.NickName = alias.NickName
	c.Category = alias.Category
	c.RegisteredClaims = jwt.RegisteredClaims{
		Issuer:    alias.Issuer,
		Subject:   alias.Subject,
		Audience:  alias.Audience,
		ExpiresAt: alias.ExpiresAt,
		NotBefore: alias.NotBefore,
		IssuedAt:  alias.IssuedAt,
		ID:        alias.ID,
	}
	return nil
}

// TokenPayload 签发新 Token 时所需传入的业务实体载荷。
type TokenPayload struct {
	// TenantID 租户编号
	TenantID string
	// UserID 用户唯一主键
	UserID int64
	// DeptID 部门主键
	DeptID int64
	// RoleID 角色主键
	RoleID string
	// RoleName 角色名称
	RoleName string
	// Account 登录账号
	Account string
	// UserName 用户真实姓名
	UserName string
	// NickName 用户昵称
	NickName string
}

// TokenResult 包含 AccessToken 与 RefreshToken 的返回实体，对标 SpringBlade OAuth2 认证响应。
type TokenResult struct {
	// AccessToken 访问接口所需携带的凭证
	AccessToken string `json:"access_token"`
	// TokenType 凭证类型，固定为 bearer
	TokenType string `json:"token_type"`
	// RefreshToken 用于刷新访问凭证的长效刷新凭证
	RefreshToken string `json:"refresh_token,omitempty"`
	// ExpiresIn 访问凭证有效时长（秒）
	ExpiresIn int64 `json:"expires_in"`
	// License 版权与签名声明信息
	License string `json:"license,omitempty"`
}

// JWTHandler JWT 编解码与校验处理器。
type JWTHandler struct {
	// SecretKey 签名密钥
	SecretKey []byte
	// AccessExpire 访问令牌过期时长
	AccessExpire time.Duration
	// RefreshExpire 刷新令牌过期时长
	RefreshExpire time.Duration
	// Issuer 令牌签发者标识
	Issuer string
	// mu 保护本地刷新令牌 JTI 状态表
	mu sync.Mutex
	// activeRefreshJTI 维护单机内存态有效 Refresh Token JTI（当 Redis 不可用时兜底防重放）
	activeRefreshJTI map[string]time.Time
}

// NewJWTHandler 创建并初始化一个 JWTHandler 处理器实例。
// secretKey: 签名私钥；accessExpire: 短期访问令牌有效时长；refreshExpire: 长期刷新令牌有效时长；issuer: 签发机构名。
func NewJWTHandler(secretKey string, accessExpire, refreshExpire time.Duration, issuer string) (*JWTHandler, error) {
	if secretKey == "" {
		return nil, ErrEmptyTokenSecret
	}
	if issuer == "" {
		issuer = "blade"
	}
	return &JWTHandler{
		SecretKey:        []byte(secretKey),
		AccessExpire:     accessExpire,
		RefreshExpire:    refreshExpire,
		Issuer:           issuer,
		activeRefreshJTI: make(map[string]time.Time),
	}, nil
}

func generateJTI() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (h *JWTHandler) storeRefreshJTI(ctx context.Context, jti string, userID int64, ttl time.Duration) error {
	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		sec := int(ttl.Seconds())
		if sec <= 0 {
			sec = 1
		}
		if err := rds.SetexCtx(ctx, RefreshTokenKeyPrefix+jti, strconv.FormatInt(userID, 10), sec); err != nil {
			return err
		}
	}
	h.mu.Lock()
	if h.activeRefreshJTI == nil {
		h.activeRefreshJTI = make(map[string]time.Time)
	}
	h.activeRefreshJTI[jti] = time.Now().Add(ttl)
	h.mu.Unlock()
	return nil
}

func (h *JWTHandler) consumeRefreshJTI(ctx context.Context, jti string) bool {
	if jti == "" {
		return false
	}
	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		res, evalErr := rds.EvalCtx(ctx, consumeRefreshLuaScript, []string{RefreshTokenKeyPrefix + jti})
		if evalErr == nil {
			if v, ok := res.(int64); ok && v == 1 {
				h.mu.Lock()
				delete(h.activeRefreshJTI, jti)
				h.mu.Unlock()
				return true
			}
			return false
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	exp, exists := h.activeRefreshJTI[jti]
	if !exists {
		return false
	}
	delete(h.activeRefreshJTI, jti)
	return time.Now().Before(exp)
}

// RevokeRefreshToken 主动撤销指定的刷新令牌，使其立即失效并写入吊销黑名单。
func (h *JWTHandler) RevokeRefreshToken(ctx context.Context, refreshTokenStr string) error {
	claims, err := h.ParseRefreshToken(refreshTokenStr)
	if err != nil {
		return err
	}
	_ = RevokeTokenWithClaims(ctx, refreshTokenStr, claims)
	if claims.ID == "" {
		return nil
	}
	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		_, _ = rds.DelCtx(ctx, RefreshTokenKeyPrefix+claims.ID)
	}
	h.mu.Lock()
	delete(h.activeRefreshJTI, claims.ID)
	h.mu.Unlock()
	return nil
}

// GenerateToken 为目标用户生成完整的双 Token 鉴权结果 (Access Token + Refresh Token)。
func (h *JWTHandler) GenerateToken(payload TokenPayload) (*TokenResult, error) {
	return h.GenerateTokenCtx(context.Background(), payload)
}

// GenerateTokenCtx 携带上下文为目标用户生成完整的双 Token 鉴权结果，并持久化 RefreshToken JTI 状态以支持单次轮换。
func (h *JWTHandler) GenerateTokenCtx(ctx context.Context, payload TokenPayload) (*TokenResult, error) {
	now := time.Now()
	accessExpiresAt := now.Add(h.AccessExpire)
	accessJTI, err := generateJTI()
	if err != nil {
		return nil, err
	}

	// 1. 构建 Access Token
	claims := BladeClaims{
		TenantID: payload.TenantID,
		UserID:   payload.UserID,
		DeptID:   payload.DeptID,
		RoleID:   payload.RoleID,
		RoleName: payload.RoleName,
		Account:  payload.Account,
		UserName: payload.UserName,
		NickName: payload.NickName,
		Category: TokenTypeAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        accessJTI,
			Issuer:    h.Issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(accessExpiresAt),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	accessTokenObj := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	accessToken, err := accessTokenObj.SignedString(h.SecretKey)
	if err != nil {
		return nil, err
	}

	// 2. 构建 Refresh Token
	var refreshToken string
	if h.RefreshExpire > 0 {
		refreshJTI, err := generateJTI()
		if err != nil {
			return nil, err
		}
		refreshClaims := BladeClaims{
			TenantID: payload.TenantID,
			UserID:   payload.UserID,
			DeptID:   payload.DeptID,
			RoleID:   payload.RoleID,
			RoleName: payload.RoleName,
			Account:  payload.Account,
			UserName: payload.UserName,
			NickName: payload.NickName,
			Category: TokenTypeRefresh,
			RegisteredClaims: jwt.RegisteredClaims{
				ID:        refreshJTI,
				Issuer:    h.Issuer,
				IssuedAt:  jwt.NewNumericDate(now),
				ExpiresAt: jwt.NewNumericDate(now.Add(h.RefreshExpire)),
				NotBefore: jwt.NewNumericDate(now),
			},
		}
		refreshObj := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
		refreshToken, err = refreshObj.SignedString(h.SecretKey)
		if err != nil {
			return nil, err
		}
		if err := h.storeRefreshJTI(ctx, refreshJTI, payload.UserID, h.RefreshExpire); err != nil {
			return nil, err
		}
	}

	return &TokenResult{
		AccessToken:  accessToken,
		TokenType:    "bearer",
		RefreshToken: refreshToken,
		ExpiresIn:    int64(h.AccessExpire.Seconds()),
		License:      "powered by go-blade",
	}, nil
}

// parseTokenRaw 内部基础解析，不检查 Category
func (h *JWTHandler) parseTokenRaw(tokenStr string) (*BladeClaims, error) {
	tokenStr = normalizeTokenString(tokenStr)

	if tokenStr == "" {
		return nil, ErrTokenInvalid
	}

	token, err := jwt.ParseWithClaims(tokenStr, &BladeClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrTokenInvalid
		}
		return h.SecretKey, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, ErrTokenInvalid
	}

	if claims, ok := token.Claims.(*BladeClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, ErrTokenInvalid
}

type claimsCtxKeyType struct{}

var claimsCtxKey = claimsCtxKeyType{}

// WithClaims 将解析出的 BladeClaims 用户凭据载荷注入到 context.Context 中。
func WithClaims(ctx context.Context, claims *BladeClaims) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, claimsCtxKey, claims)
}

// FromClaims 从 context.Context 中提取当前用户的 BladeClaims 载荷信息。
// 若未登录或提取失败，返回 nil。
func FromClaims(ctx context.Context) *BladeClaims {
	if ctx == nil {
		return nil
	}
	if claims, ok := ctx.Value(claimsCtxKey).(*BladeClaims); ok {
		return claims
	}
	return nil
}

// TokenValidator 定义统一的访问令牌解析与验签接口，兼容对称 HMAC 与非对称 RSA JWKS。
type TokenValidator interface {
	// ValidateToken 校验并解析访问令牌，返回 BladeClaims。
	ValidateToken(ctx context.Context, tokenStr string) (*BladeClaims, error)
}

// CompositeValidator 组合多个 TokenValidator，按顺序尝试验签（支持 JWKS 与 HMAC 双模无缝回退）。
type CompositeValidator struct {
	validators []TokenValidator
}

// NewCompositeValidator 构造支持多验签器回退的组合验签器。
func NewCompositeValidator(validators ...TokenValidator) *CompositeValidator {
	filtered := make([]TokenValidator, 0, len(validators))
	for _, v := range validators {
		if v != nil {
			filtered = append(filtered, v)
		}
	}
	return &CompositeValidator{validators: filtered}
}

// ValidateToken 依次调用内部验签器解析 Token；若检测到过期、被吊销或类型不匹配则立即返回对应错误。
func (c *CompositeValidator) ValidateToken(ctx context.Context, tokenStr string) (*BladeClaims, error) {
	if c == nil || len(c.validators) == 0 {
		return nil, ErrTokenInvalid
	}
	if IsTokenRevoked(ctx, tokenStr) {
		return nil, ErrTokenRevoked
	}
	var lastErr error
	for _, v := range c.validators {
		claims, err := v.ValidateToken(ctx, tokenStr)
		if err == nil {
			return claims, nil
		}
		if errors.Is(err, ErrTokenExpired) || errors.Is(err, ErrTokenTypeMismatch) || errors.Is(err, ErrTokenRevoked) {
			return nil, err
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrTokenInvalid
}

// ValidateToken 实现 TokenValidator 接口，校验并解析客户端传入的访问令牌 (Access Token)。
func (h *JWTHandler) ValidateToken(ctx context.Context, tokenStr string) (*BladeClaims, error) {
	return h.ParseTokenCtx(ctx, tokenStr)
}

// ParseToken 校验并解析客户端传入的访问令牌 (Access Token)，提取 BladeClaims 载荷信息。
// 必须严格校验 Category 为 access_token，拒绝空值、已吊销令牌或 Refresh Token，确保权限严格隔离。
func (h *JWTHandler) ParseToken(tokenStr string) (*BladeClaims, error) {
	return h.ParseTokenCtx(context.Background(), tokenStr)
}

// ParseTokenCtx 携带上下文校验并解析访问令牌，自动检查令牌是否已被主动吊销。
func (h *JWTHandler) ParseTokenCtx(ctx context.Context, tokenStr string) (*BladeClaims, error) {
	if IsTokenRevoked(ctx, tokenStr) {
		return nil, ErrTokenRevoked
	}
	claims, err := h.parseTokenRaw(tokenStr)
	if err != nil {
		return nil, err
	}
	if claims.Category != TokenTypeAccess {
		return nil, ErrTokenTypeMismatch
	}
	return claims, nil
}

// ParseRefreshToken 校验并解析客户端传入的长效刷新令牌 (Refresh Token)。
func (h *JWTHandler) ParseRefreshToken(refreshTokenStr string) (*BladeClaims, error) {
	if IsTokenRevoked(context.Background(), refreshTokenStr) {
		return nil, ErrTokenRevoked
	}
	claims, err := h.parseTokenRaw(refreshTokenStr)
	if err != nil {
		return nil, err
	}
	if claims.Category != TokenTypeRefresh {
		return nil, ErrTokenTypeMismatch
	}
	return claims, nil
}

// RefreshToken 使用有效的长效刷新令牌换发一套全新的双令牌，并原子作废旧的刷新令牌。
func (h *JWTHandler) RefreshToken(refreshTokenStr string) (*TokenResult, error) {
	return h.RefreshTokenCtx(context.Background(), refreshTokenStr)
}

// RefreshTokenCtx 携带上下文使用有效的长效刷新令牌换发一套全新的双令牌，并原子作废旧的刷新令牌（单次轮换防重放）。
func (h *JWTHandler) RefreshTokenCtx(ctx context.Context, refreshTokenStr string) (*TokenResult, error) {
	if IsTokenRevoked(ctx, refreshTokenStr) {
		return nil, ErrTokenRevoked
	}
	claims, err := h.ParseRefreshToken(refreshTokenStr)
	if err != nil {
		return nil, err
	}

	if claims.ID == "" || !h.consumeRefreshJTI(ctx, claims.ID) {
		return nil, ErrRefreshTokenRevoked
	}

	payload := TokenPayload{
		TenantID: claims.TenantID,
		UserID:   claims.UserID,
		DeptID:   claims.DeptID,
		RoleID:   claims.RoleID,
		RoleName: claims.RoleName,
		Account:  claims.Account,
		UserName: claims.UserName,
		NickName: claims.NickName,
	}

	return h.GenerateTokenCtx(ctx, payload)
}
