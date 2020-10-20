package utils

import (
	"time"
)

const (
	TT             = "2006-01-02 15:04:05"
	MINUTE_SECONDS = 60    //每分钟的秒数
	HOUR_SECONDS   = 3600  //每小时的秒数
	DAY_SECONDS    = 86400 //每天的秒数
)

type HoDay int32

const (
	HO_NONE    HoDay = 0
	HO_YUANDAN HoDay = 1
	HO_ERTONG  HoDay = 2
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
	return time.ParseInLocation(TT, t, time.Local)
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

// DayCross 经历了多少天，取整
func DayCross(sec int64) int32 {
	now := time.Now()
	bf := time.Unix(sec, 0)
	return int32(now.Sub(bf).Seconds() / DAY_SECONDS)
}

// GetHoDay 当天是不是摸个节日
func GetDayHo() HoDay {
	now := time.Now()
	if now.Month() == 1 && now.Day() == 1 {
		return HO_YUANDAN
	} else if now.Month() == 6 && now.Day() == 1 {
		return HO_ERTONG
	} else if now.Month() == 5 && now.Day() == 1 {
		return HO_ERTONG
	} else if now.Month() == 10 && now.Day() == 1 {
		return HO_ERTONG
	}
	return HO_NONE
}

// EndTimeSec 取结束时间,d是秒
func EndTimeSec(sec int64, d int64) time.Time {
	bf := time.Unix(sec, 0)
	return bf.Add(time.Second * time.Duration(d))
}

// EndTimeSec 取结束时间,d是秒
func EndTimeStr(t string, d int64) time.Time {
	bf, _ := TimeFromStr(t)
	return bf.Add(time.Second * time.Duration(d))
}
