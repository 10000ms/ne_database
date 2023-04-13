package resource

type MemoryConfig struct {

}

func (c *MemoryConfig) Reader(offset int64) []byte {
	return nil
}

func (c *MemoryConfig) Writer(offset int64, data []byte) bool {
	return false
}
