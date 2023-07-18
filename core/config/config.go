package config

import (
	"encoding/json"
	"fmt"
	"ne_database/utils"
	"os"
)

type config struct {
	init bool

	Dev      bool `json:"Dev"`      // 是否处在开发模式
	PageSize int  `json:"PageSize"` // 数据一页的大小
}

func (c *config) Init() error {
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
				return err
			}
			err = c.InitByJSON(string(rawConfig))
			return err
		}
		utils.LogSystem(fmt.Sprintf("初始化配置完成，目前的配置是：%s", utils.ToJSON(c)))
		c.init = true
	}
	return nil
}

func (c *config) InitByJSON(jsonConfig string) error {
	err := json.Unmarshal([]byte(jsonConfig), c)
	if err != nil {
		utils.LogError("读取JSON配置错误", err)
		return err
	}
	utils.LogSystem("读取JSON配置完成")
	return nil
}

func (c *config) InitToDefault() {
	c.Dev = false
	c.PageSize = 64000 // go中是按照byte计算的
}

var CoreConfig = config{}