package config

import (
	"encoding/json"
	"fmt"
	"os"

	"ne_database/core/base"
	"ne_database/utils"
)

type config struct {
	init bool

	Dev      bool   `json:"Dev"`      // 是否处在开发模式
	PageSize int    `json:"PageSize"` // 数据一页的大小
	FileAddr string `json:"FileAddr"` // 数据文件存放目录
}

func (c *config) Init() base.StandardError {
	if c.init != true {
		// 先给配置项初始值
		c.InitToDefault()

		// 再读取配置文件的值，覆盖初始值
		var ConfigFilePath string
		ConfigFilePath = os.Getenv("CONFIG_PATH")
		if ConfigFilePath != "" {
			utils.LogSystem(fmt.Sprintf("获取到配置文件地址: %s", ConfigFilePath))
			utils.LogSystem("开始从配置文件加载配置信息...")

			rawConfig, err := os.ReadFile(ConfigFilePath)
			if err != nil {
				utils.LogError("获取不到配置文件", err)
				return base.NewDBError(base.FunctionModelCoreConfig, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
			}
			er := c.InitByJSON(string(rawConfig))
			return er
		}
		utils.LogSystem(fmt.Sprintf("初始化配置完成，目前的配置是：%s", utils.ToJSON(c)))
		c.init = true
	}
	return nil
}

func (c *config) InitByJSON(jsonConfig string) base.StandardError {
	err := json.Unmarshal([]byte(jsonConfig), c)
	if err != nil {
		utils.LogError("读取JSON配置错误", err)
		return base.NewDBError(base.FunctionModelCoreConfig, base.ErrorTypeConfig, base.ErrorBaseCodeConfigError, err)
	}
	utils.LogSystem("读取JSON配置完成")
	return nil
}

func (c *config) InitToDefault() {
	c.Dev = false
	c.PageSize = 64000 // go中是按照byte计算的
	c.FileAddr = "./"
}

var CoreConfig = config{}

// init 配置自动初始化
func init() {
	_ = CoreConfig.Init()
}
