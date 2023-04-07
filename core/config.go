package core

import (
	"encoding/json"
	"io/ioutil"
	"ne_database/utils"
	"os"
)

type config struct {
	init bool

	Dev bool `json:"Dev"`
}

func (c *config) Init() error {
	if c.init != true {
		// 先给配置项初始值
		c.Dev = false

		// 再读取配置文件的值，覆盖初始值
		var ConfigFilePath string
		ConfigFilePath = os.Getenv("CONFIG_PATH")
		if ConfigFilePath != "" {
			utils.LogSystem("获取到配置文件地址: %s")
			utils.LogSystem("开始从配置文件加载配置信息...")

			rawConfig, err := ioutil.ReadFile(ConfigFilePath)
			if err != nil {
				utils.LogFatal("获取不到配置文件！")
				return err
			}
			err = json.Unmarshal(rawConfig, c)
			if err != nil {
				utils.LogFatal("获取不到配置文件！")
				return err
			}
		}
		utils.LogSystem("初始化配置完成，目前的配置是：%s", utils.ToJSON(c))
		c.init = true
	}
	return nil
}

var CoreConfig = config{}
