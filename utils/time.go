package utils

import (
	"time"
)

const (
	TT = "2006-01-02 15:04:05"
)

// TimeNowStr 当前时间到字符串
func TimeNowStr() string {
	return time.Now().Format(TT)
}

// TimeNowSec 当前时间到秒
func TimeNowSec() int64 {
	return time.Now().Unix()
}

// FormatTime 格式化时间
func FormatTime(t time.Time) string {
	return t.Format(TT)
}

// TimeFromSec 秒数到时间
func TimeFromSec(sec int64) time.Time {
	return time.Unix(sec, 0)
}

// TimeFromStr 字符串到时间
func TimeFromStr(t string) (time.Time, error) {
	return time.Parse(TT, t)
}

// TodayZero 今天的0点
func TodayZero() time.Time {
	now := time.Now()
	return time.Date(now.Year(),
		now.Month(),
		now.Day(),
		0, 0, 0, 0, time.Local)
}

// TomorrowZero 明天的0点
func TomorrowZero() time.Time {
	now := time.Now()
	return time.Date(now.Year(),
		now.Month(),
		now.Day()+1,
		0, 0, 0, 0, time.Local)
}

// DayCrossZero 经历了多少个自然天
func DayCrossZero(sec int64) int32 {
	now := time.Now()
	nday := time.Date(now.Year(),
		now.Month(),
		now.Day(),
		0, 0, 0, 0, time.Local)

	bf := time.Unix(sec, 0)
	bday := time.Date(bf.Year(),
		bf.Month(),
		bf.Day()+1,
		0, 0, 0, 0, time.Local)

	d := int32(0)
	for bday.Unix() <= nday.Unix() {
		d = d + 1
		bday = bday.AddDate(0, 0, 1)
	}
	return d
}
