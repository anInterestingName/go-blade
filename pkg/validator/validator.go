// Package validator 提供零外部依赖、基于 Go 标准库反射的结构化参数校验器。
// 支持结构体标签校验（required, min, max, len, email, phone, oneof 等），
// 并在校验不通过时自动提取字段中文标签（label/json），输出包装为 *xerr.CodeError 的友好中文提示。
package validator

import (
	"context"
	"fmt"
	"net/mail"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/go-blade/pkg/xerr"
)

// phoneRegexp 中国大陆 11 位手机号码正则表达式。
var phoneRegexp = regexp.MustCompile(`^1[3-9]\d{9}$`)

// ValidateStruct 带上下文首参的结构化参数校验函数（符合工程上下文规范）。
// 若校验不通过，返回码值为 xerr.RequestParamError (400) 的 *xerr.CodeError。
func ValidateStruct(ctx context.Context, v any) error {
	_ = ctx
	return Validate(v)
}

// Validate 对传入的结构体或结构体指针执行标签规则校验。
// 支持标签格式：`validate:"required,min=3,max=20,len=11,email,phone,oneof=1 2"`，
// 字段显示名称优先读取 `label` 标签，其次读取 `json` 标签，最后回退为结构体字段名。
// 若校验失败，返回 *xerr.CodeError（错误码为 xerr.RequestParamError）。
func Validate(v any) error {
	if v == nil {
		return xerr.NewCodeError(xerr.RequestParamError, "请求参数不能为空")
	}

	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return xerr.NewCodeError(xerr.RequestParamError, "请求参数不能为空")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return xerr.NewCodeError(xerr.RequestParamError, "请求参数必须为结构体")
	}

	return validateStructValue(val)
}

// validateStructValue 递归校验结构体各个导出字段。
func validateStructValue(val reflect.Value) error {
	typ := val.Type()
	numField := val.NumField()

	for i := 0; i < numField; i++ {
		fieldType := typ.Field(i)
		// 忽略非导出字段
		if fieldType.PkgPath != "" {
			continue
		}

		fieldVal := val.Field(i)
		tag := strings.TrimSpace(fieldType.Tag.Get("validate"))
		if tag == "-" {
			continue
		}

		label := extractFieldLabel(fieldType)

		if tag != "" {
			if err := validateFieldByTag(fieldVal, label, tag); err != nil {
				return err
			}
		}

		// 处理嵌套结构体或结构体指针的递归校验
		if err := validateNested(fieldVal); err != nil {
			return err
		}
	}

	return nil
}

// validateNested 对嵌套结构体、结构体指针及结构体切片进行递归校验。
func validateNested(fieldVal reflect.Value) error {
	v := fieldVal
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		return validateStructValue(v)
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i)
			for elem.Kind() == reflect.Ptr {
				if elem.IsNil() {
					break
				}
				elem = elem.Elem()
			}
			if elem.IsValid() && elem.Kind() == reflect.Struct {
				if err := validateStructValue(elem); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// extractFieldLabel 提取字段的友好显示名称：优先 label 标签，其次 json 标签，最后字段名。
func extractFieldLabel(field reflect.StructField) string {
	if label := strings.TrimSpace(field.Tag.Get("label")); label != "" {
		return label
	}
	if jsonTag := strings.TrimSpace(field.Tag.Get("json")); jsonTag != "" {
		parts := strings.Split(jsonTag, ",")
		name := strings.TrimSpace(parts[0])
		if name != "" && name != "-" {
			return name
		}
	}
	return field.Name
}

// validateFieldByTag 按照逗号分隔的规则列表校验单个字段。
func validateFieldByTag(fieldVal reflect.Value, label, tag string) error {
	rules := strings.Split(tag, ",")

	// 解引用指针查看是否有值
	isNilPtr := false
	v := fieldVal
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			isNilPtr = true
			break
		}
		v = v.Elem()
	}

	hasRequired := false
	for _, rawRule := range rules {
		rule := strings.TrimSpace(rawRule)
		if rule == "required" {
			hasRequired = true
			break
		}
	}

	if isNilPtr || isEmptyValue(v) {
		if hasRequired {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s不能为空", label))
		}
		// 非必填且为空值时，跳过后续格式与范围校验
		return nil
	}

	for _, rawRule := range rules {
		rule := strings.TrimSpace(rawRule)
		if rule == "" || rule == "required" {
			continue
		}

		name, param, _ := strings.Cut(rule, "=")
		name = strings.TrimSpace(name)
		param = strings.TrimSpace(param)

		if err := applyRule(v, label, name, param); err != nil {
			return err
		}
	}

	return nil
}

// isEmptyValue 判断字段值是否为空（零值）。
func isEmptyValue(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.String:
		return strings.TrimSpace(v.String()) == ""
	case reflect.Slice, reflect.Map, reflect.Array:
		return v.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

// applyRule 对非空字段执行具体的单条校验规则。
func applyRule(v reflect.Value, label, ruleName, param string) error {
	switch ruleName {
	case "min":
		return checkMin(v, label, param)
	case "max":
		return checkMax(v, label, param)
	case "len":
		return checkLen(v, label, param)
	case "email":
		return checkEmail(v, label)
	case "phone":
		return checkPhone(v, label)
	case "oneof":
		return checkOneOf(v, label, param)
	default:
		return nil
	}
}

func checkMin(v reflect.Value, label, param string) error {
	switch v.Kind() {
	case reflect.String:
		minVal, err := strconv.Atoi(param)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if utf8.RuneCountInString(v.String()) < minVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s长度不能少于 %d 个字符", label, minVal))
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		minVal, err := strconv.Atoi(param)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if v.Len() < minVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s元素数量不能少于 %d 个", label, minVal))
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		minVal, err := strconv.ParseInt(param, 10, 64)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if v.Int() < minVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s不能小于 %d", label, minVal))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		minVal, err := strconv.ParseUint(param, 10, 64)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if v.Uint() < minVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s不能小于 %d", label, minVal))
		}
	case reflect.Float32, reflect.Float64:
		minVal, err := strconv.ParseFloat(param, 64)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if v.Float() < minVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s不能小于 %s", label, param))
		}
	}
	return nil
}

func checkMax(v reflect.Value, label, param string) error {
	switch v.Kind() {
	case reflect.String:
		maxVal, err := strconv.Atoi(param)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if utf8.RuneCountInString(v.String()) > maxVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s长度不能超过 %d 个字符", label, maxVal))
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		maxVal, err := strconv.Atoi(param)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if v.Len() > maxVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s元素数量不能超过 %d 个", label, maxVal))
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		maxVal, err := strconv.ParseInt(param, 10, 64)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if v.Int() > maxVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s不能大于 %d", label, maxVal))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		maxVal, err := strconv.ParseUint(param, 10, 64)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if v.Uint() > maxVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s不能大于 %d", label, maxVal))
		}
	case reflect.Float32, reflect.Float64:
		maxVal, err := strconv.ParseFloat(param, 64)
		if err != nil {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
		}
		if v.Float() > maxVal {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s不能大于 %s", label, param))
		}
	}
	return nil
}

func checkLen(v reflect.Value, label, param string) error {
	expectedLen, err := strconv.Atoi(param)
	if err != nil {
		return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s校验规则配置错误", label))
	}

	switch v.Kind() {
	case reflect.String:
		if utf8.RuneCountInString(v.String()) != expectedLen {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s长度必须为 %d 个字符", label, expectedLen))
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		if v.Len() != expectedLen {
			return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s元素数量必须为 %d 个", label, expectedLen))
		}
	}
	return nil
}

func checkEmail(v reflect.Value, label string) error {
	if v.Kind() != reflect.String {
		return nil
	}
	str := strings.TrimSpace(v.String())
	addr, err := mail.ParseAddress(str)
	if err != nil || addr.Address != str || !strings.Contains(addr.Address, "@") {
		return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s格式不正确", label))
	}
	return nil
}

func checkPhone(v reflect.Value, label string) error {
	if v.Kind() != reflect.String {
		return nil
	}
	if !phoneRegexp.MatchString(strings.TrimSpace(v.String())) {
		return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s格式不正确", label))
	}
	return nil
}

func checkOneOf(v reflect.Value, label, param string) error {
	options := strings.Fields(param)
	if len(options) == 0 {
		return nil
	}

	var current string
	switch v.Kind() {
	case reflect.String:
		current = v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		current = strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		current = strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		current = strconv.FormatFloat(v.Float(), 'f', -1, 64)
	default:
		current = fmt.Sprintf("%v", v.Interface())
	}

	for _, opt := range options {
		if current == opt {
			return nil
		}
	}

	return xerr.NewCodeError(xerr.RequestParamError, fmt.Sprintf("%s必须是 [%s] 中的一个", label, strings.Join(options, " ")))
}
