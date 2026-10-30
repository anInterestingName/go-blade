// Package validator 提供零外部依赖、基于 Go 标准库反射的结构化参数校验器单元测试。
package validator

import (
	"context"
	"strings"
	"testing"

	"github.com/go-blade/pkg/xerr"
)

type sampleProfile struct {
	City string `json:"city" label:"所在城市" validate:"required,min=2,max=10"`
}

type sampleUserReq struct {
	Account  string        `json:"account" label:"用户名" validate:"required,min=3,max=20"`
	Code     string        `json:"code" label:"验证码" validate:"required,len=6"`
	Email    string        `json:"email" label:"邮箱" validate:"email"`
	Phone    string        `json:"phone" label:"手机号" validate:"required,phone"`
	Age      int           `json:"age" label:"年龄" validate:"required,min=18,max=120"`
	Score    float64       `json:"score" label:"评分" validate:"min=1.5,max=10.0"`
	Status   int           `json:"status" label:"状态" validate:"required,oneof=1 2"`
	Role     string        `json:"role" validate:"oneof=admin user guest"`
	RoleIDs  []uint64      `json:"role_ids" label:"角色列表" validate:"required,min=1,max=3"`
	Profile  sampleProfile `json:"profile"`
	Ignored  string        `validate:"-"`
	internal string
}

func validSampleUser() sampleUserReq {
	return sampleUserReq{
		Account: "blade_admin",
		Code:    "123456",
		Email:   "admin@goblade.cn",
		Phone:   "13812345678",
		Age:     25,
		Score:   9.5,
		Status:  1,
		Role:    "admin",
		RoleIDs: []uint64{101, 102},
		Profile: sampleProfile{
			City: "杭州",
		},
		internal: "skip",
	}
}

func assertCodeError(t *testing.T, err error, expectedSubstring string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", expectedSubstring)
	}
	ce, ok := err.(*xerr.CodeError)
	if !ok {
		t.Fatalf("expected *xerr.CodeError, got %T (%v)", err, err)
	}
	if ce.GetCode() != xerr.RequestParamError {
		t.Errorf("expected code %d, got %d", xerr.RequestParamError, ce.GetCode())
	}
	if !strings.Contains(ce.GetMsg(), expectedSubstring) {
		t.Errorf("expected msg containing %q, got %q", expectedSubstring, ce.GetMsg())
	}
}

func TestValidate_Success(t *testing.T) {
	req := validSampleUser()
	if err := Validate(req); err != nil {
		t.Fatalf("expected valid struct to pass, got: %v", err)
	}
	if err := ValidateStruct(context.Background(), &req); err != nil {
		t.Fatalf("expected valid struct pointer to pass, got: %v", err)
	}
}

func TestValidate_NilOrNonStruct(t *testing.T) {
	assertCodeError(t, Validate(nil), "请求参数不能为空")

	var nilPtr *sampleUserReq
	assertCodeError(t, Validate(nilPtr), "请求参数不能为空")

	assertCodeError(t, Validate("not_a_struct"), "请求参数必须为结构体")
}

func TestValidate_StringRules(t *testing.T) {
	// required
	req := validSampleUser()
	req.Account = "   "
	assertCodeError(t, Validate(req), "用户名不能为空")

	// min (中文与 ASCII 字符数)
	req = validSampleUser()
	req.Account = "ab"
	assertCodeError(t, Validate(req), "用户名长度不能少于 3 个字符")

	// max
	req = validSampleUser()
	req.Account = strings.Repeat("a", 21)
	assertCodeError(t, Validate(req), "用户名长度不能超过 20 个字符")

	// len
	req = validSampleUser()
	req.Code = "12345"
	assertCodeError(t, Validate(req), "验证码长度必须为 6 个字符")

	// email
	req = validSampleUser()
	req.Email = "invalid-email"
	assertCodeError(t, Validate(req), "邮箱格式不正确")

	// optional email when empty should pass
	req = validSampleUser()
	req.Email = ""
	if err := Validate(req); err != nil {
		t.Fatalf("expected empty optional email to pass, got: %v", err)
	}

	// phone
	req = validSampleUser()
	req.Phone = "12812345678"
	assertCodeError(t, Validate(req), "手机号格式不正确")

	req.Phone = "1381234567"
	assertCodeError(t, Validate(req), "手机号格式不正确")
}

func TestValidate_NumberAndOneOfRules(t *testing.T) {
	// int required
	req := validSampleUser()
	req.Age = 0
	assertCodeError(t, Validate(req), "年龄不能为空")

	// int min
	req = validSampleUser()
	req.Age = 16
	assertCodeError(t, Validate(req), "年龄不能小于 18")

	// int max
	req = validSampleUser()
	req.Age = 150
	assertCodeError(t, Validate(req), "年龄不能大于 120")

	// float min/max
	req = validSampleUser()
	req.Score = 1.0
	assertCodeError(t, Validate(req), "评分不能小于 1.5")

	req.Score = 11.0
	assertCodeError(t, Validate(req), "评分不能大于 10.0")

	// int oneof
	req = validSampleUser()
	req.Status = 3
	assertCodeError(t, Validate(req), "状态必须是 [1 2] 中的一个")

	// string oneof + json tag fallback for label
	req = validSampleUser()
	req.Role = "superadmin"
	assertCodeError(t, Validate(req), "role必须是 [admin user guest] 中的一个")
}

func TestValidate_SliceAndNestedRules(t *testing.T) {
	// slice required
	req := validSampleUser()
	req.RoleIDs = nil
	assertCodeError(t, Validate(req), "角色列表不能为空")

	// slice max
	req = validSampleUser()
	req.RoleIDs = []uint64{1, 2, 3, 4}
	assertCodeError(t, Validate(req), "角色列表元素数量不能超过 3 个")

	// nested struct validation
	req = validSampleUser()
	req.Profile.City = ""
	assertCodeError(t, Validate(req), "所在城市不能为空")

	req.Profile.City = "京"
	assertCodeError(t, Validate(req), "所在城市长度不能少于 2 个字符")
}
