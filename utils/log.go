package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
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

type logDevConfig struct {
	IsInit      bool
	InLogDev    bool
	LowestLevel int
	Modules     []string
}

func (l *logDevConfig) Init() {
	l.InLogDev = os.Getenv("LOG_DEV") != ""

	l.LowestLevel = -1
	levelString := os.Getenv("LOG_DEV_LEVEL")
	if levelString != "" {
		l.LowestLevel, _ = strconv.Atoi(levelString)
	}

	l.Modules = make([]string, 0)
	moduleString := os.Getenv("LOG_DEV_MODULES")
	if moduleString != "" {
		l.Modules = strings.Split(moduleString, ",")
	}

	l.IsInit = true
}

var logDevManger = logDevConfig{}

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

// NilLogFunc 空方法，不做任何操作
func NilLogFunc(value ...interface{}) {

}

// LogDev 用于开发过程中的日志输出
// 用法 utils.LogDev("BPlusTree", 1)("需要print的信息")
// level 只能是1-10
func LogDev(module string, level int) func(...interface{}) {
	if logDevManger.IsInit != true {
		logDevManger.Init()
	}
	if level > 10 {
		level = 10
	} else if level < 1 {
		level = 1
	}
	if !logDevManger.InLogDev || level < logDevManger.LowestLevel {
		return NilLogFunc
	}
	if len(logDevManger.Modules) > 0 {
		canModulePrint := false
		for _, m := range logDevManger.Modules {
			if module == m {
				canModulePrint = true
				break
			}
		}
		if canModulePrint != true {
			return NilLogFunc
		}
	}

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
