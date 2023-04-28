package resource

type Config interface {
	Reader(offset int64) ([]byte, error)
	Writer(offset int64, data []byte) (bool, error)
}
