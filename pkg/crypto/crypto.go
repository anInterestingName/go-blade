// Package crypto 提供企业级字段级透明对称加解密 (AES-256-GCM)、HMAC-SHA256 密文精确检索盲索引、
// GORM EncryptedString 透明落盘序列化类型，以及基于结构体 Tag (`mask:"phone|email|id_card|bank_card|name|secret"`) 的响应数据脱敏能力。
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/go-blade/pkg/auditlog"
)

const (
	// CipherPrefix 标记字符串已经过 AES-256-GCM 加密的标准版本前缀，防止二次重复加密。
	CipherPrefix = "enc:v1:"
	// defaultMasterSecret 未显式初始化全局密钥时的兜底默认主密钥。
	defaultMasterSecret = "go-blade-default-aes256-master-key"
)

var (
	keyMu          sync.RWMutex
	defaultAEAD    cipher.AEAD
	defaultHMACKey []byte
)

func init() {
	_ = SetGlobalKey(defaultMasterSecret)
}

// SetGlobalKey 设置全局默认的字段级加解密与盲索引主密钥（通过 SHA-256 派生出标准 32 字节 AES-256 密钥）。
func SetGlobalKey(secret string) error {
	clean := strings.TrimSpace(secret)
	if clean == "" {
		return errors.New("crypto master secret must not be empty")
	}
	derived := sha256.Sum256([]byte(clean))
	block, err := aes.NewCipher(derived[:])
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	hmacSeed := sha256.Sum256([]byte("hmac-blind-index:" + clean))

	keyMu.Lock()
	defaultAEAD = aead
	defaultHMACKey = hmacSeed[:]
	keyMu.Unlock()
	return nil
}

// Encrypt 使用全局 AES-256-GCM 密钥加密明文字符串，输出带 `"enc:v1:"` 前缀的 Base64 密文。
// 若输入为空或已携带 `"enc:v1:"` 前缀，则幂等原样返回。
func Encrypt(plaintext string) (string, error) {
	if plaintext == "" || strings.HasPrefix(plaintext, CipherPrefix) {
		return plaintext, nil
	}
	keyMu.RLock()
	aead := defaultAEAD
	keyMu.RUnlock()
	if aead == nil {
		return "", errors.New("crypto cipher is not initialized")
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce failed: %w", err)
	}

	sealed := aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return CipherPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt 解密带有 `"enc:v1:"` 前缀的 AES-256-GCM 密文；若输入未携带该前缀，则视为历史明文数据直接返回。
func Decrypt(ciphertext string) (string, error) {
	if ciphertext == "" || !strings.HasPrefix(ciphertext, CipherPrefix) {
		return ciphertext, nil
	}
	rawB64 := strings.TrimPrefix(ciphertext, CipherPrefix)
	data, err := base64.StdEncoding.DecodeString(rawB64)
	if err != nil {
		return "", fmt.Errorf("decode cipher base64 failed: %w", err)
	}

	keyMu.RLock()
	aead := defaultAEAD
	keyMu.RUnlock()
	if aead == nil {
		return "", errors.New("crypto cipher is not initialized")
	}

	nonceSize := aead.NonceSize()
	if len(data) < nonceSize {
		return "", errors.New("ciphertext too short")
	}

	nonce, encrypted := data[:nonceSize], data[nonceSize:]
	opened, err := aead.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return "", fmt.Errorf("aes-gcm decrypt failed: %w", err)
	}
	return string(opened), nil
}

// BlindIndex 计算敏感明文（如手机号、身份证号）的 HMAC-SHA256 确定性盲索引摘要（64 位十六进制字符串）。
// 用于在数据库中对加密列建立等值查询索引（如 `WHERE phone_index = ?`），避免暴露原始明文。
func BlindIndex(plaintext string) string {
	trimmed := strings.TrimSpace(plaintext)
	if trimmed == "" {
		return ""
	}
	keyMu.RLock()
	hmacKey := defaultHMACKey
	keyMu.RUnlock()

	mac := hmac.New(sha256.New, hmacKey)
	_, _ = mac.Write([]byte(trimmed))
	return hex.EncodeToString(mac.Sum(nil))
}

// EncryptedString 实现 GORM `driver.Valuer` 与 `sql.Scanner` 接口的透明加密字符串类型。
// 写入数据库时自动执行 AES-256-GCM 加密，从数据库查询读取时自动解密还原为明文。
type EncryptedString string

// Value 实现 driver.Valuer 接口，在写入 SQL 数据库前自动加密。
func (es EncryptedString) Value() (driver.Value, error) {
	if es == "" {
		return "", nil
	}
	return Encrypt(string(es))
}

// Scan 实现 sql.Scanner 接口，在从 SQL 数据库读取后自动解密。
func (es *EncryptedString) Scan(value any) error {
	if value == nil {
		*es = ""
		return nil
	}
	var raw string
	switch v := value.(type) {
	case string:
		raw = v
	case []byte:
		raw = string(v)
	default:
		raw = fmt.Sprintf("%v", v)
	}
	plain, err := Decrypt(raw)
	if err != nil {
		return err
	}
	*es = EncryptedString(plain)
	return nil
}

// String 返回解密后的明文字符串。
func (es EncryptedString) String() string {
	return string(es)
}

// MaskEmail 对电子邮箱地址进行掩码脱敏（例如 `"alice@example.com"` -> `"a***e@example.com"`）。
func MaskEmail(email string) string {
	trimmed := strings.TrimSpace(email)
	atIdx := strings.IndexByte(trimmed, '@')
	if atIdx <= 0 {
		return trimmed
	}
	local := []rune(trimmed[:atIdx])
	domain := trimmed[atIdx:]
	if len(local) == 1 {
		return string(local[0]) + "***" + domain
	}
	return string(local[0]) + "***" + string(local[len(local)-1]) + domain
}

// MaskBankCard 对银行卡号进行掩码脱敏（保留前 4 位与后 4 位，中间替换为 `"********"`）。
func MaskBankCard(card string) string {
	trimmed := strings.TrimSpace(card)
	runes := []rune(trimmed)
	if len(runes) > 8 {
		return string(runes[:4]) + "********" + string(runes[len(runes)-4:])
	}
	return auditlog.MaskPhone(trimmed)
}

// MaskChineseName 对姓名进行掩码脱敏（例如 `"张三"` -> `"张*"`，`"欧阳修"` -> `"欧*修"`）。
func MaskChineseName(name string) string {
	trimmed := strings.TrimSpace(name)
	runes := []rune(trimmed)
	n := len(runes)
	if n <= 1 {
		return trimmed
	}
	if n == 2 {
		return string(runes[0]) + "*"
	}
	return string(runes[0]) + strings.Repeat("*", n-2) + string(runes[n-1])
}

// MaskByRule 根据指定的脱敏规则名称（phone, email, id_card, bank_card, name, secret）对字符串进行脱敏。
func MaskByRule(rule, val string) string {
	if val == "" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(rule)) {
	case "phone", "mobile":
		return auditlog.MaskPhone(val)
	case "id_card", "idcard":
		return auditlog.MaskIDCard(val)
	case "email":
		return MaskEmail(val)
	case "bank_card", "bankcard":
		return MaskBankCard(val)
	case "name", "real_name":
		return MaskChineseName(val)
	case "secret", "password", "token":
		return auditlog.MaskedPlaceholder
	default:
		return val
	}
}

// MaskStruct 递归扫描结构体指针（或结构体切片指针）中含有 `mask:"..."` 标签的字符串字段并原地执行脱敏。
func MaskStruct(target any) {
	if target == nil {
		return
	}
	maskReflectValue(reflect.ValueOf(target))
}

func maskReflectValue(v reflect.Value) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			fieldVal := v.Field(i)
			fieldType := t.Field(i)
			if !fieldVal.CanSet() {
				continue
			}
			tag := strings.TrimSpace(fieldType.Tag.Get("mask"))
			if tag != "" && fieldVal.Kind() == reflect.String {
				fieldVal.SetString(MaskByRule(tag, fieldVal.String()))
				continue
			}
			maskReflectValue(fieldVal)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			maskReflectValue(v.Index(i))
		}
	}
}
