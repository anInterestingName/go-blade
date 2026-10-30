package crypto

import (
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type sampleSecretAccount struct {
	ID         int64           `gorm:"primaryKey"`
	Phone      EncryptedString `gorm:"column:phone;size:255"`
	PhoneIndex string          `gorm:"column:phone_index;size:64;index"`
}

func TestAESGCM_And_GormEncryptedString(t *testing.T) {
	require.NoError(t, SetGlobalKey("unit-test-master-secret-2026"))

	cipherText, err := Encrypt("13812345678")
	require.NoError(t, err)
	assert.True(t, len(cipherText) > len(CipherPrefix))
	assert.Contains(t, cipherText, CipherPrefix)

	// 幂等防重复加密验证
	cipherAgain, err := Encrypt(cipherText)
	require.NoError(t, err)
	assert.Equal(t, cipherText, cipherAgain)

	plainText, err := Decrypt(cipherText)
	require.NoError(t, err)
	assert.Equal(t, "13812345678", plainText)

	// 验证在 GORM SQLite 中的透明加解密与盲索引精确查找
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&sampleSecretAccount{}))

	rawPhone := "13988887777"
	acc := sampleSecretAccount{
		ID:         1,
		Phone:      EncryptedString(rawPhone),
		PhoneIndex: BlindIndex(rawPhone),
	}
	require.NoError(t, db.Create(&acc).Error)

	// 1. 直接查询底层原始列值，验证数据库中存储的是 enc:v1: 密文而非明文
	var rawStored string
	require.NoError(t, db.Table("sample_secret_accounts").Select("phone").Where("id = ?", 1).Scan(&rawStored).Error)
	assert.NotEqual(t, rawPhone, rawStored)
	assert.Contains(t, rawStored, CipherPrefix)

	// 2. 通过 BlindIndex 盲索引查询并用 GORM Scan 到 EncryptedString，验证自动解密还原为明文
	var loaded sampleSecretAccount
	require.NoError(t, db.Where("phone_index = ?", BlindIndex(rawPhone)).First(&loaded).Error)
	assert.Equal(t, rawPhone, loaded.Phone.String())
}

type sampleUserMaskVO struct {
	RealName string `mask:"name"`
	Phone    string `mask:"phone"`
	Email    string `mask:"email"`
	IDCard   string `mask:"id_card"`
	BankCard string `mask:"bank_card"`
	Secret   string `mask:"secret"`
}

func TestMaskStruct_AllRules(t *testing.T) {
	list := []*sampleUserMaskVO{
		{
			RealName: "诸葛孔明",
			Phone:    "13812345678",
			Email:    "kongming@blade.io",
			IDCard:   "110101199003078888",
			BankCard: "6222021234567890123",
			Secret:   "my-private-token",
		},
	}

	MaskStruct(&list)

	assert.Equal(t, "诸**明", list[0].RealName)
	assert.Equal(t, "138****5678", list[0].Phone)
	assert.Equal(t, "k***g@blade.io", list[0].Email)
	assert.Equal(t, "110101********8888", list[0].IDCard)
	assert.Equal(t, "6222********0123", list[0].BankCard)
	assert.Equal(t, "******", list[0].Secret)
}
