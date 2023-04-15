package tableSchema

type TableMetaInfo struct {
	Name             string
	PrimaryKeyName   string
	PrimaryKeyParser *MetaParser
	ValueName        []string
	ValueParserMap   map[string]*MetaParser
}

// InitTableMetaInfo
// 确定一个表，需要：
// 1. 主键名称，及其类型和长度
// 2. 详细的值（们）的名称，及其类型和长度
func InitTableMetaInfo(name string) *TableMetaInfo {
	return nil
}
