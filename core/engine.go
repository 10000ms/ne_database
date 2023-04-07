package core

type Engine struct {
}

// Init 初始化方法
func (e *Engine) Init() error {
	var err error

	err = CoreConfig.Init()
	if err != nil {
		return err
	}
	return nil
}
