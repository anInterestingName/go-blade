// Package redis 提供基于 go-zero 的 Redis 客户端单例封装与防重放验证码存储能力。
package redis

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math/big"
	"strings"
	"time"
)

var (
	// ErrInvalidCaptchaLength 表示请求的验证码长度非法（必须在 4 至 6 位之间）。
	ErrInvalidCaptchaLength = errors.New("captcha length must be between 4 and 6")
)

const (
	// CaptchaKeyPrefix 验证码在 Redis 中存储的统一键前缀。
	CaptchaKeyPrefix = "blade:captcha:"
	// DefaultCaptchaLength 默认验证码长度。
	DefaultCaptchaLength = 4
	// DefaultCaptchaExpire 默认验证码过期时间（5分钟）。
	DefaultCaptchaExpire = 5 * time.Minute
)

// CaptchaResult 验证码生成结果实体。
type CaptchaResult struct {
	// Key 验证码唯一凭证（UUID/HEX字符串），用于回传比对。
	Key string `json:"key"`
	// Code 纯数字验证码字符串。
	Code string `json:"code"`
	// Image 包含真实随机验证码图形的 Base64 编码 PNG 数据 (data:image/png;base64,...)。
	Image string `json:"image"`
	// ExpireIn 凭据有效持续时间。
	ExpireIn time.Duration `json:"expire_in"`
}

// GenerateCaptcha 生成包含默认 4 位数字的验证码实体及真实图形 Base64，并以默认 5 分钟写入 Redis 缓存。
func GenerateCaptcha(ctx context.Context, expire time.Duration) (*CaptchaResult, error) {
	return GenerateCaptchaWithLength(ctx, DefaultCaptchaLength, expire)
}

// GenerateCaptchaWithLength 生成指定长度纯数字验证码及真实图形 Base64 并写入 Redis 缓存。
func GenerateCaptchaWithLength(ctx context.Context, length int, expire time.Duration) (*CaptchaResult, error) {
	if length < 4 || length > 6 {
		return nil, ErrInvalidCaptchaLength
	}
	if expire <= 0 {
		expire = DefaultCaptchaExpire
	}

	r, err := GetRedis()
	if err != nil {
		return nil, err
	}

	keyHex, err := generateRandomHex(16)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random captcha key: %w", err)
	}

	code, err := generateNumericCode(length)
	if err != nil {
		return nil, fmt.Errorf("failed to generate numeric code: %w", err)
	}

	imgBase64, err := renderCaptchaPNG(code)
	if err != nil {
		return nil, fmt.Errorf("failed to render captcha image: %w", err)
	}

	redisKey := CaptchaKeyPrefix + keyHex
	expireSeconds := int(expire.Seconds())
	if expireSeconds <= 0 {
		expireSeconds = 1
	}

	if err := r.SetexCtx(ctx, redisKey, code, expireSeconds); err != nil {
		return nil, fmt.Errorf("failed to save captcha to redis: %w", err)
	}

	return &CaptchaResult{
		Key:      keyHex,
		Code:     code,
		Image:    imgBase64,
		ExpireIn: expire,
	}, nil
}

// verifyCaptchaLuaScript 使用 Lua 脚本保证验证码比对与单次消费 (clear) 的原子性，避免并发击穿防重放。
// 返回值：1 表示匹配成功并已清理，0 表示不匹配或不存在
const verifyCaptchaLuaScript = `
local stored = redis.call('GET', KEYS[1])
if not stored then
    return 0
end
if string.lower(stored) == string.lower(ARGV[1]) then
    if ARGV[2] == '1' then
        redis.call('DEL', KEYS[1])
    end
    return 1
else
    return 0
end
`

// VerifyCaptcha 校验指定 key 的验证码是否匹配。
// 如果 clear 为 true 且校验成功，将通过 Lua 脚本原子删除该 Key，坚决防止并发重复提交重放。
// 校验忽略大小写与首尾空格。
func VerifyCaptcha(ctx context.Context, key, code string, clear bool) (bool, error) {
	cleanKey := strings.TrimSpace(key)
	cleanCode := strings.TrimSpace(code)
	if cleanKey == "" || cleanCode == "" {
		return false, nil
	}

	r, err := GetRedis()
	if err != nil {
		return false, err
	}

	redisKey := CaptchaKeyPrefix + cleanKey
	clearFlag := "0"
	if clear {
		clearFlag = "1"
	}

	res, err := r.EvalCtx(ctx, verifyCaptchaLuaScript, []string{redisKey}, cleanCode, clearFlag)
	if err != nil {
		return false, fmt.Errorf("failed to eval verify captcha script: %w", err)
	}

	// Redis 脚本返回 1 代表匹配成功
	if intVal, ok := res.(int64); ok && intVal == 1 {
		return true, nil
	}

	return false, nil
}

// generateNumericCode 生成指定位数的纯数字随机验证码字符串。
func generateNumericCode(length int) (string, error) {
	const digits = "0123456789"
	b := make([]byte, length)
	maxVal := big.NewInt(int64(len(digits)))
	for i := 0; i < length; i++ {
		num, err := rand.Int(rand.Reader, maxVal)
		if err != nil {
			return "", err
		}
		b[i] = digits[num.Int64()]
	}
	return string(b), nil
}

// generateRandomHex 生成指定字节数的十六进制随机字符串。
func generateRandomHex(byteCount int) (string, error) {
	b := make([]byte, byteCount)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// digitGlyphs 定义 0-9 数字的 5x7 点阵字模，用于无外部依赖的验证码图像绘制。
var digitGlyphs = map[byte][7]uint8{
	'0': {0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E},
	'1': {0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E},
	'2': {0x0E, 0x11, 0x01, 0x06, 0x08, 0x10, 0x1F},
	'3': {0x1F, 0x02, 0x04, 0x06, 0x01, 0x11, 0x0E},
	'4': {0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02},
	'5': {0x1F, 0x10, 0x1E, 0x01, 0x01, 0x11, 0x0E},
	'6': {0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E},
	'7': {0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08},
	'8': {0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E},
	'9': {0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C},
}

// renderCaptchaPNG 根据验证码文本绘制 120x40 彩色带噪点 PNG 图像并返回 Data URL。
func renderCaptchaPNG(code string) (string, error) {
	const (
		width  = 120
		height = 40
		scale  = 3
	)
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	// 1. 填充浅色背景
	bgColor := color.RGBA{R: 245, G: 247, B: 250, A: 255}
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.SetRGBA(x, y, bgColor)
		}
	}

	// 2. 绘制随机背景噪点
	noiseBytes := make([]byte, 120)
	_, _ = rand.Read(noiseBytes)
	for i := 0; i+2 < len(noiseBytes); i += 3 {
		nx := int(noiseBytes[i]) % width
		ny := int(noiseBytes[i+1]) % height
		c := color.RGBA{
			R: 150 + noiseBytes[i]%80,
			G: 150 + noiseBytes[i+1]%80,
			B: 150 + noiseBytes[i+2]%80,
			A: 255,
		}
		img.SetRGBA(nx, ny, c)
	}

	// 3. 绘制每个验证码数字字符
	palette := []color.RGBA{
		{R: 24, G: 90, B: 188, A: 255},
		{R: 185, G: 45, B: 60, A: 255},
		{R: 30, G: 130, B: 76, A: 255},
		{R: 120, G: 50, B: 160, A: 255},
	}

	totalWidth := len(code)*5*scale + (len(code)-1)*6
	startX := (width - totalWidth) / 2
	if startX < 6 {
		startX = 6
	}
	baseY := (height - 7*scale) / 2

	for idx := 0; idx < len(code); idx++ {
		glyph, ok := digitGlyphs[code[idx]]
		if !ok {
			continue
		}
		fg := palette[idx%len(palette)]
		charX := startX + idx*(5*scale+6)
		for row := 0; row < 7; row++ {
			rowBits := glyph[row]
			for col := 0; col < 5; col++ {
				if (rowBits>>(4-col))&1 == 1 {
					for dy := 0; dy < scale; dy++ {
						for dx := 0; dx < scale; dx++ {
							px := charX + col*scale + dx
							py := baseY + row*scale + dy
							if px >= 0 && px < width && py >= 0 && py < height {
								img.SetRGBA(px, py, fg)
							}
						}
					}
				}
			}
		}
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return "", err
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}
