package resource

type IOManager interface {
	Reader(offset int64) ([]byte, error)
	Writer(offset int64, data []byte) (bool, error)
}
