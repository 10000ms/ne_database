package utils

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"time"
)

func log(level string, value []interface{}, color string) {
	now := time.Now()
	dateString := fmt.Sprintf(
		"%d-%d-%d %d:%d:%d.%d",
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
		now.Second(),
		now.Nanosecond(),
	)
	var info string
	for _, i := range value {
		info += fmt.Sprintf("%s", i)
	}
	str := fmt.Sprintf("%s %s %s", dateString, level, info)

	// 分行分割字符串后再打印
	arr := strings.Split(str, "\n")
	for _, line := range arr {
		var lineStr string
		if color != "" {
			lineStr = fmt.Sprintf("\u001B[1;0;%sm%s\u001B[0m", color, line)
		}
		fmt.Println(lineStr)
	}
}

func LogDebug(value ...interface{}) {
	log("[DEBUG]", value, "35")
}

func LogInfo(value ...interface{}) {
	log("[INFO]", value, "")
}

func LogWarning(value ...interface{}) {
	log("[WARNING]", value, "33")
}

func LogError(value ...interface{}) {
	color := "31"
	log("[ERROR]", value, color)
	log("", []interface{}{debug.Stack()}, color)
}

func LogFatal(value ...interface{}) {
	color := "31"
	log("[FATAL]", value, color)
	log("", []interface{}{debug.Stack()}, color)
}

func LogSystem(value ...interface{}) {
	var info string
	for _, i := range value {
		info += fmt.Sprintf("%s", i)
	}
	fmt.Printf("\033[1;0;36m%s\033[0m\n", info)
}

func ToJSON(data interface{}) string {
	dataByte, err := json.Marshal(data)
	if err != nil {
		LogError(fmt.Errorf("ToJSON 错误 err: %s", err.Error()))
		return fmt.Sprintf("%v", data)
	}

	return string(dataByte)
}
