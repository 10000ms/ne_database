package tableSchema

import (
	"encoding/json"
	"fmt"

	"ne_database/core/base"
	"ne_database/utils"
)

type FieldInfo struct {
	Name         string   `json:"name"`
	Length       int      `json:"length"`
	FieldType    MetaType `json:"-"`
	RawFieldType string   `json:"raw_field_type"`
}

type TableMetaInfo struct {
	Name                string       `json:"name"`
	PrimaryKeyFieldInfo *FieldInfo   `json:"primary_key_field_info"`
	ValueFieldInfo      []*FieldInfo `json:"value_field_info"`
}

func (info *FieldInfo) Verification() base.StandardError {
	t := info.FieldType
	switch t.GetType() {
	case base.DBDataTypeInt64:
		if info.Length != base.DataByteLengthInt64 {
			utils.LogError(fmt.Sprintf("[Verification] 类型<%s>校验错误, 类型长度错误: %d", t.GetType(), info.Length))
			return base.NewDBError(base.FunctionModelCoreDTableSchema, base.ErrorTypeType, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("int64类型长度错误: %d", info.Length))
		}
	}
	return nil
}

// InitTableMetaInfo
// 确定一个表，需要：
// 1. 主键名称，及其类型和长度
// 2. 详细的值（们）的名称，及其类型和长度
func InitTableMetaInfo(name string) *TableMetaInfo {
	return nil
}

func RawToFieldType(raw string) (MetaType, base.StandardError) {
	switch raw {
	case string(base.DBDataTypeInt64):
		return Int64Type, nil
	case string(base.DBDataTypeString):
		return StringType, nil
	default:
		utils.LogError(fmt.Sprintf("[RawToFieldType] 错误的RawFieldType: %s", raw))
		return nil, base.NewDBError(base.FunctionModelCoreDTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("错误的RawFieldType: %s", raw))
	}
}

// InitTableMetaInfoByJson 通过 json 初始化一个 TableMetaInfo
func InitTableMetaInfoByJson(metaJson string) (*TableMetaInfo, base.StandardError) {
	r := &TableMetaInfo{}
	er := json.Unmarshal([]byte(metaJson), r)
	if er != nil {
		utils.LogError(fmt.Sprintf("[InitTableMetaInfoByJson] json解析错误: %s", er.Error()))
		return nil, base.NewDBError(base.FunctionModelCoreDTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, er)
	}
	// 替换主键的 FieldType 为真实
	pkFieldType, err := RawToFieldType(r.PrimaryKeyFieldInfo.RawFieldType)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreDTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] primaryKey RawToFieldType出错, %s", err.Error()))
		return nil, err
	}
	r.PrimaryKeyFieldInfo.FieldType = pkFieldType
	err = r.PrimaryKeyFieldInfo.Verification()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreDTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] PrimaryKeyFieldInfo.Verification出错, %s", err.Error()))
		return nil, err
	}

	// 替换值的 FieldType 为真实
	for _, v := range r.ValueFieldInfo {
		valueFieldType, err := RawToFieldType(v.RawFieldType)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreDTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] value RawToFieldType出错, %s", err.Error()))
			return nil, err
		}
		v.FieldType = valueFieldType
		err = v.Verification()
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreDTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] value.Verification出错, %s", err.Error()))
			return nil, err
		}
	}

	return r, nil
}
