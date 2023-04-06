package utils

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"time"
)

const (
	fgBlack = iota + 30
	fgRed
	fgGreen
	fgYellow
	fgBlue
	fgMagenta
	fgCyan
	fgWhite
)

func log(level string, value []interface{}, color int) {
	now := time.Now()
	dateString := fmt.Sprintf(
		"%d-%d-%d %d:%d:%d.%d",
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
		now.Second(),
		now.Nanosecond()/100000,
	)
	var (
		info    string
		strInfo string
	)
	for _, i := range value {
		info += fmt.Sprintf("%s", i)
	}

	// 有level才有时间输出
	if level == "" {
		strInfo = fmt.Sprintf("%s", info)
	} else {
		strInfo = fmt.Sprintf("%s %s %s", dateString, level, info)
	}

	// 分行分割字符串后再打印
	arr := strings.Split(strInfo, "\n")
	for _, line := range arr {
		var lineStr string
		if color != 0 {
			lineStr = fmt.Sprintf("\u001B[1;0;%dm%s\u001B[0m", color, line)
		}
		fmt.Println(lineStr)
	}
}

// LogDev 用于开发过程中的日志输出
// 用法 utils.LogDev("BPlusTree", 1)("需要print的信息")
func LogDev(module string, level int) func(...interface{}) {
	// TODO：判断只有dev开启
	// TODO：module、level都要进行判断，符合条件的才输出
	return func(value ...interface{}) {
		log("[Dev Info]", value, fgGreen)
	}
}

func LogDebug(value ...interface{}) {
	log("[DEBUG]", value, fgMagenta)
}

func LogInfo(value ...interface{}) {
	log("[INFO]", value, 0)
}

func LogWarning(value ...interface{}) {
	log("[WARNING]", value, fgYellow)
}

func LogError(value ...interface{}) {
	color := fgRed
	log("[ERROR]", value, color)
	log("", []interface{}{debug.Stack()}, color)
}

func LogFatal(value ...interface{}) {
	color := fgRed
	log("[FATAL]", value, color)
	log("", []interface{}{debug.Stack()}, color)
}

func LogSystem(value ...interface{}) {
	log("", []interface{}{debug.Stack()}, fgCyan)
}

func ToJSON(data interface{}) string {
	dataByte, err := json.Marshal(data)
	if err != nil {
		LogError(fmt.Errorf("ToJSON 错误 err: %s", err.Error()))
		return fmt.Sprintf("%v", data)
	}

	return string(dataByte)
}
