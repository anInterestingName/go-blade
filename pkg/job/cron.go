// Package job 提供零外部依赖的标准 5 段（分 时 日 月 周）与 6 段（秒 分 时 日 月 周）Cron 表达式解析器及调度支持。
package job

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrInvalidCronSpec 表示传入的 Cron 表达式格式不合法。
	ErrInvalidCronSpec = errors.New("invalid cron expression")
)

// CronSchedule 定义计算下一次定时触发时间的标准接口。
type CronSchedule interface {
	// Next 计算在给定时间 t 之后的下一个触发时刻（严格大于 t）。
	Next(t time.Time) time.Time
}

type intervalSchedule struct {
	interval time.Duration
}

func (s intervalSchedule) Next(t time.Time) time.Time {
	return t.Add(s.interval)
}

// specSchedule 基于位掩码 (Bitmask) 实现的 6 字段（秒 分 时 日 月 周）日历调度器。
type specSchedule struct {
	second uint64 // bits 0..59
	minute uint64 // bits 0..59
	hour   uint64 // bits 0..23
	dom    uint64 // bits 1..31
	month  uint64 // bits 1..12
	dow    uint64 // bits 0..6 (Sunday=0 .. Saturday=6)
}

// ParseCron 解析标准 Cron 表达式并返回 CronSchedule。
// 支持格式：
// 1. 宏描述符：`@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`, `@every <duration>`
// 2. 标准 5 段式：`minute hour dom month dow`（默认秒固定为 0）
// 3. 扩展 6 段式：`second minute hour dom month dow`
// 每个字段均支持 `*`、`,`（列表）、`-`（范围）与 `/`（步长）。
func ParseCron(spec string) (CronSchedule, error) {
	clean := strings.TrimSpace(spec)
	if clean == "" {
		return nil, ErrInvalidCronSpec
	}

	if strings.HasPrefix(clean, "@") {
		return parseDescriptor(clean)
	}

	fields := strings.Fields(clean)
	var secField, minField, hourField, domField, monthField, dowField string
	switch len(fields) {
	case 5:
		secField = "0"
		minField, hourField, domField, monthField, dowField = fields[0], fields[1], fields[2], fields[3], fields[4]
	case 6:
		secField, minField, hourField, domField, monthField, dowField = fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]
	default:
		return nil, fmt.Errorf("%w: expected 5 or 6 fields, got %d in %q", ErrInvalidCronSpec, len(fields), spec)
	}

	secMask, err := parseCronField(secField, 0, 59)
	if err != nil {
		return nil, err
	}
	minMask, err := parseCronField(minField, 0, 59)
	if err != nil {
		return nil, err
	}
	hourMask, err := parseCronField(hourField, 0, 23)
	if err != nil {
		return nil, err
	}
	domMask, err := parseCronField(domField, 1, 31)
	if err != nil {
		return nil, err
	}
	monthMask, err := parseCronField(monthField, 1, 12)
	if err != nil {
		return nil, err
	}
	// 周字段允许 0..7（其中 7 也映射到周日 0）
	dowRawMask, err := parseCronField(dowField, 0, 7)
	if err != nil {
		return nil, err
	}
	if dowRawMask&(1<<7) != 0 {
		dowRawMask |= 1 << 0
		dowRawMask &^= 1 << 7
	}

	return &specSchedule{
		second: secMask,
		minute: minMask,
		hour:   hourMask,
		dom:    domMask,
		month:  monthMask,
		dow:    dowRawMask,
	}, nil
}

func parseDescriptor(desc string) (CronSchedule, error) {
	lower := strings.ToLower(desc)
	switch lower {
	case "@yearly", "@annually":
		return ParseCron("0 0 0 1 1 *")
	case "@monthly":
		return ParseCron("0 0 0 1 * *")
	case "@weekly":
		return ParseCron("0 0 0 * * 0")
	case "@daily", "@midnight":
		return ParseCron("0 0 0 * * *")
	case "@hourly":
		return ParseCron("0 0 * * * *")
	}

	if strings.HasPrefix(lower, "@every ") {
		durStr := strings.TrimSpace(desc[len("@every "):])
		d, err := time.ParseDuration(durStr)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%w: invalid @every duration %q", ErrInvalidCronSpec, durStr)
		}
		return intervalSchedule{interval: d}, nil
	}

	return nil, fmt.Errorf("%w: unrecognized descriptor %q", ErrInvalidCronSpec, desc)
}

func parseCronField(field string, minVal, maxVal int) (uint64, error) {
	if field == "" {
		return 0, ErrInvalidCronSpec
	}
	var mask uint64
	parts := strings.Split(field, ",")
	for _, part := range parts {
		partMask, err := parseCronRange(strings.TrimSpace(part), minVal, maxVal)
		if err != nil {
			return 0, err
		}
		mask |= partMask
	}
	if mask == 0 {
		return 0, ErrInvalidCronSpec
	}
	return mask, nil
}

func parseCronRange(expr string, minVal, maxVal int) (uint64, error) {
	if expr == "" {
		return 0, ErrInvalidCronSpec
	}

	step := 1
	rangeExpr := expr
	if slashIdx := strings.IndexByte(expr, '/'); slashIdx != -1 {
		rangeExpr = expr[:slashIdx]
		stepStr := expr[slashIdx+1:]
		parsedStep, err := strconv.Atoi(stepStr)
		if err != nil || parsedStep <= 0 {
			return 0, fmt.Errorf("%w: invalid step %q", ErrInvalidCronSpec, expr)
		}
		step = parsedStep
	}

	var start, end int
	if rangeExpr == "*" || rangeExpr == "?" {
		start = minVal
		end = maxVal
	} else if dashIdx := strings.IndexByte(rangeExpr, '-'); dashIdx != -1 {
		var err error
		start, err = strconv.Atoi(rangeExpr[:dashIdx])
		if err != nil {
			return 0, fmt.Errorf("%w: invalid range start in %q", ErrInvalidCronSpec, expr)
		}
		end, err = strconv.Atoi(rangeExpr[dashIdx+1:])
		if err != nil {
			return 0, fmt.Errorf("%w: invalid range end in %q", ErrInvalidCronSpec, expr)
		}
	} else {
		val, err := strconv.Atoi(rangeExpr)
		if err != nil {
			return 0, fmt.Errorf("%w: invalid value %q", ErrInvalidCronSpec, expr)
		}
		start = val
		if strings.Contains(expr, "/") {
			end = maxVal
		} else {
			end = val
		}
	}

	if start < minVal || end > maxVal || start > end {
		return 0, fmt.Errorf("%w: value out of bounds [%d-%d] in %q", ErrInvalidCronSpec, minVal, maxVal, expr)
	}

	var mask uint64
	for i := start; i <= end; i += step {
		mask |= 1 << uint(i)
	}
	return mask, nil
}

// Next 计算在给定时间 t 之后的下一个匹配 Cron 日历规则的整秒时刻。
func (s *specSchedule) Next(t time.Time) time.Time {
	// 从下一秒整点开始向后搜索（上限 5 年，防止闰年 2 月 29 日越界）
	cand := t.Add(time.Second - time.Duration(t.Nanosecond())*time.Nanosecond)
	yearLimit := t.Year() + 5

WRAP:
	if cand.Year() > yearLimit {
		return time.Time{}
	}

	// 1. 匹配月份 (1..12)
	for (s.month & (1 << uint(cand.Month()))) == 0 {
		cand = time.Date(cand.Year(), cand.Month()+1, 1, 0, 0, 0, 0, cand.Location())
		if cand.Year() > yearLimit {
			return time.Time{}
		}
	}

	// 2. 匹配日期与星期
	for !s.dayMatches(cand) {
		m := cand.Month()
		cand = time.Date(cand.Year(), cand.Month(), cand.Day()+1, 0, 0, 0, 0, cand.Location())
		if cand.Month() != m {
			goto WRAP
		}
	}

	// 3. 匹配小时 (0..23)
	for (s.hour & (1 << uint(cand.Hour()))) == 0 {
		d := cand.Day()
		cand = time.Date(cand.Year(), cand.Month(), cand.Day(), cand.Hour()+1, 0, 0, 0, cand.Location())
		if cand.Day() != d {
			goto WRAP
		}
	}

	// 4. 匹配分钟 (0..59)
	for (s.minute & (1 << uint(cand.Minute()))) == 0 {
		h := cand.Hour()
		cand = cand.Truncate(time.Minute).Add(time.Minute)
		if cand.Hour() != h {
			goto WRAP
		}
	}

	// 5. 匹配秒 (0..59)
	for (s.second & (1 << uint(cand.Second()))) == 0 {
		min := cand.Minute()
		cand = cand.Truncate(time.Second).Add(time.Second)
		if cand.Minute() != min {
			goto WRAP
		}
	}

	return cand
}

func (s *specSchedule) dayMatches(t time.Time) bool {
	domBit := uint64(1) << uint(t.Day())
	dowBit := uint64(1) << uint(t.Weekday())
	return (s.dom&domBit) != 0 && (s.dow&dowBit) != 0
}
