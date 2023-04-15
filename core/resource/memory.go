package resource

type MemoryConfig struct {
	Storage map[int64][]byte
}

func InitMemoryConfig(initData map[int64][]byte) *MemoryConfig {
	c := MemoryConfig{}

	if initData != nil {
		c.Storage = initData
	} else {
		c.Storage = make(map[int64][]byte)
	}

	return &c
}

func (c *MemoryConfig) Reader(offset int64) []byte {
	if page, ok := c.Storage[offset]; ok {
		return page
	}
	return nil
}

func (c *MemoryConfig) Writer(offset int64, data []byte) bool {
	c.Storage[offset] = data
	return true
}
