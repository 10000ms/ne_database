package tableSchema

type FieldInfo struct {
	Name      string
	Length    int
	FieldType *MetaType
}

type TableMetaInfo struct {
	Name                string
	PrimaryKeyFieldInfo *FieldInfo
	ValueFieldInfo      []*FieldInfo
}

// InitTableMetaInfo
// 确定一个表，需要：
// 1. 主键名称，及其类型和长度
// 2. 详细的值（们）的名称，及其类型和长度
func InitTableMetaInfo(name string) *TableMetaInfo {
	return nil
}
