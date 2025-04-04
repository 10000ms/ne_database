package utils

import (
	"encoding/json"
	"fmt"
	"ne_database/utils/set"
	"os"
	"runtime"
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

	LogModuleDevOSKey        = "LOG_DEV"
	LogModuleLogModulesOSKey = "LOG_DEV_MODULES"
)

type logDevConfig struct {
	IsInit   bool
	InLogDev bool
	Modules  *set.StringsSet
}

func (l *logDevConfig) Init() {
	l.InLogDev = os.Getenv(LogModuleDevOSKey) != ""

	m := make([]string, 0)
	moduleString := os.Getenv(LogModuleLogModulesOSKey)
	if moduleString != "" {
		m = strings.Split(moduleString, ",")
	}
	l.Modules = set.NewStringsSet(m...)
	l.IsInit = true
}

var logDevManger = logDevConfig{}

func getFuncName(p string) string {
	index := strings.LastIndexByte(p, '/')
	if index != -1 {
		p = p[index:]
		index = strings.IndexByte(p, '.')
		if index != -1 {
			p = strings.TrimPrefix(p[index:], ".")
		}
	}
	return p
}

func log(level string, value []interface{}, color int, depth int) {
	now := time.Now()
	pc, f, l, ok := runtime.Caller(depth)
	if !ok {
		f = "UNkOWN"
		l = 1
	} else {
		s := strings.LastIndex(f, "/")
		if s >= 0 {
			f = f[s+1:]
		}
	}
	dateString := fmt.Sprintf(
		"%d-%d-%d %d:%d:%d.%d [%s:%d:%s]",
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
		now.Second(),
		now.Nanosecond()/100000,
		f,
		l,
		getFuncName(runtime.FuncForPC(pc).Name()),
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
func LogDev(module string) func(...interface{}) {
	if logDevManger.IsInit != true {
		logDevManger.Init()
	}
	if !logDevManger.InLogDev {
		return NilLogFunc
	}
	if logDevManger.Modules.Contain(module) || logDevManger.Modules.Contain("All") {
		return func(value ...interface{}) {
			log("[Dev Info]", value, fgGreen, 2)
		}
	} else {
		return NilLogFunc
	}
}

func LogDebug(value ...interface{}) {
	log("[DEBUG]", value, fgMagenta, 2)
}

func LogInfo(value ...interface{}) {
	log("[INFO]", value, 0, 2)
}

func LogWarning(value ...interface{}) {
	log("[WARNING]", value, fgYellow, 2)
}

func LogError(value ...interface{}) {
	color := fgRed
	log("[ERROR]", value, color, 2)
	log("", []interface{}{debug.Stack()}, color, 2)
}

func LogFatal(value ...interface{}) {
	color := fgRed
	log("[FATAL]", value, color, 2)
	log("", []interface{}{debug.Stack()}, color, 2)
}

func LogSystem(value ...interface{}) {
	log("", value, fgCyan, 2)
}

func LogWithoutInfo(value ...interface{}) {
	log("", value, fgMagenta, 2)
}

func ToJSON(data interface{}) string {
	dataByte, err := json.Marshal(data)
	if err != nil {
		LogError(fmt.Errorf("ToJSON 错误 err: %s", err.Error()))
		return fmt.Sprintf("%v", data)
	}

	return string(dataByte)
}

// init log 自动初始化
func init() {
	logDevManger.Init()
}
