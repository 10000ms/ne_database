package tableSchema

import (
	"encoding/json"
	"fmt"

	"ne_database/core/base"
	"ne_database/utils"
	"ne_database/utils/set"
)

type FieldInfo struct {
	Name         string   `json:"name"`
	Length       int      `json:"length"`
	FieldType    MetaType `json:"-"`
	DefaultValue string   `json:"default"` // 这个值是建表语句的原始值，使用需要进行处理
	RawFieldType string   `json:"type"`
}

type TableMetaInfo struct {
	Name                string       `json:"name"`
	PrimaryKeyFieldInfo *FieldInfo   `json:"primary_key"`
	ValueFieldInfo      []*FieldInfo `json:"value"`
}

// Verification 值配置校验
func (info *FieldInfo) Verification() base.StandardError {
	t := info.FieldType
	switch t.GetType() {
	case base.DBDataTypeInt64:
		if info.Length != base.DataByteLengthInt64 {
			utils.LogError(fmt.Sprintf("[Verification] 类型<%s>校验错误, 类型长度错误: %d", t.GetType(), info.Length))
			return base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeType, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("int64类型长度错误: %d", info.Length))
		}
	}
	return nil
}

func (info *FieldInfo) CompareFieldInfo(info2 *FieldInfo) bool {
	if info.Name != info2.Name {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 5)("[CompareFieldInfo] 值信息：名称不一致")
		return false
	}
	if info.Length != info2.Length {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 5)("[CompareFieldInfo] 值信息：长度不一致")
		return false
	}
	if info.FieldType != info2.FieldType {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 5)("[CompareFieldInfo] 值信息：类型不一致")
		return false
	}
	if info.DefaultValue != info2.DefaultValue {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 5)("[CompareFieldInfo] 值信息：默认值不一致")
		return false
	}
	return true
}

func (info *TableMetaInfo) ValueFieldInfoMap() (map[string]*FieldInfo, base.StandardError) {
	valueFieldInfoMap := make(map[string]*FieldInfo)
	for _, i := range info.ValueFieldInfo {
		valueFieldInfoMap[i.Name] = i
	}
	return valueFieldInfoMap, nil
}

// Verification 表配置校验
func (info *TableMetaInfo) Verification() base.StandardError {
	if info.Name == "" {
		utils.LogError(fmt.Sprintf("[Verification] 表校验错误, 表名为空"))
		return base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeType, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("表名为空"))
	}
	if info.PrimaryKeyFieldInfo == nil {
		utils.LogError(fmt.Sprintf("[Verification] 表校验错误, 主键配置为空"))
		return base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeType, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("主键配置为空"))
	}
	err := info.PrimaryKeyFieldInfo.Verification()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[Verification] 表校验错误 primaryKey info Verification 出错, %s", err.Error()))
		return err
	}
	if info.ValueFieldInfo == nil || len(info.ValueFieldInfo) == 0 {
		utils.LogError(fmt.Sprintf("[Verification] 表校验错误, 值配置为空"))
		return base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeType, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("值配置为空"))
	}
	existName := set.NewStringSet()
	for _, i := range info.ValueFieldInfo {
		if i == nil {
			utils.LogError(fmt.Sprintf("[Verification] 表校验错误, 值配置为空"))
			return base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeType, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("值配置为空"))
		}
		err := i.Verification()
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[Verification] 表校验错误 value info Verification 出错, %s", err.Error()))
			return err
		}
		if existName.Contains(i.Name) {
			utils.LogError(fmt.Sprintf("[Verification] 表校验错误, 值配置名<%s>重复", i.Name))
			return base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeType, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("值配置名<%s>重复", i.Name))
		}
		existName.Add(i.Name)
	}
	return nil
}

func (info *TableMetaInfo) CompareTableInfo(info2 *TableMetaInfo) bool {
	// 1. 对比name
	if info.Name != info2.Name {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 5)("[CompareTableInfo] 两表名称不一致")
		return false
	}

	// 2. 对比PrimaryKeyFieldInfo
	if !info.PrimaryKeyFieldInfo.CompareFieldInfo(info2.PrimaryKeyFieldInfo) {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 5)("[CompareTableInfo] 两表主键不一致")
		return false
	}

	// 3. 对比ValueFieldInfo
	if len(info.ValueFieldInfo) != len(info2.ValueFieldInfo) {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 5)("[CompareTableInfo] 两表值数量不一致")
		return false
	}
	for i, v1 := range info.ValueFieldInfo {
		v2 := info2.ValueFieldInfo[i]
		if !v1.CompareFieldInfo(v2) {
			utils.LogDev(string(base.FunctionModelCoreTableSchema), 5)("[CompareTableInfo] 两表值不一致")
			return false
		}
	}
	return true
}

func (info *TableMetaInfo) FillingRawFieldType() base.StandardError {
	var err base.StandardError

	info.PrimaryKeyFieldInfo.RawFieldType, err = FieldTypeToRaw(info.PrimaryKeyFieldInfo.FieldType)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[TableMetaInfo.FillingRawFieldType] 获取RawFieldType出错, %s", err.Error()))
		return err
	}

	if info.ValueFieldInfo == nil {
		info.ValueFieldInfo = make([]*FieldInfo, 0)
	}
	for _, i := range info.ValueFieldInfo {
		if i != nil {
			i.RawFieldType, err = FieldTypeToRaw(i.FieldType)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[TableMetaInfo.FillingRawFieldType] 获取RawFieldType出错, %s", err.Error()))
				return err
			}
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
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf("错误的RawFieldType: %s", raw))
	}
}

func FieldTypeToRaw(fieldType MetaType) (string, base.StandardError) {
	switch fieldType {
	case Int64Type:
		return string(base.DBDataTypeInt64), nil
	case StringType:
		return string(base.DBDataTypeString), nil
	default:
		utils.LogError(fmt.Sprintf("[FieldTypeToRaw] 错误的fieldType: %#v", fieldType))
		return "", base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeType, base.ErrorBaseCodeInnerTypeError, fmt.Errorf("错误的fieldType: %#v", fieldType))
	}
}

// InitTableMetaInfoByJson 通过 json 初始化一个 TableMetaInfo
func InitTableMetaInfoByJson(metaJson string) (*TableMetaInfo, base.StandardError) {
	r := &TableMetaInfo{}
	er := json.Unmarshal([]byte(metaJson), r)
	if er != nil {
		utils.LogError(fmt.Sprintf("[InitTableMetaInfoByJson] json解析错误: %s", er.Error()))
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, er)
	}
	if r.PrimaryKeyFieldInfo == nil || r.ValueFieldInfo == nil || len(r.ValueFieldInfo) == 0 {
		utils.LogError(fmt.Sprintf("[InitTableMetaInfoByJson] 表结构内容缺失, 获取json为: %s, 解析后为: %s", metaJson, utils.ToJSON(r)))
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, er)
	}
	// 替换主键的 FieldType 为真实
	pkFieldType, err := RawToFieldType(r.PrimaryKeyFieldInfo.RawFieldType)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] primaryKey RawToFieldType出错, %s", err.Error()))
		return nil, err
	}
	r.PrimaryKeyFieldInfo.FieldType = pkFieldType
	err = r.PrimaryKeyFieldInfo.Verification()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] PrimaryKeyFieldInfo.Verification出错, %s", err.Error()))
		return nil, err
	}

	// 替换值的 FieldType 为真实
	for _, v := range r.ValueFieldInfo {
		valueFieldType, err := RawToFieldType(v.RawFieldType)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] value RawToFieldType出错, %s", err.Error()))
			return nil, err
		}
		v.FieldType = valueFieldType
		err = v.Verification()
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] value.Verification出错, %s", err.Error()))
			return nil, err
		}
	}

	err = r.Verification()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 1)(fmt.Sprintf("[InitTableMetaInfoByJson] table.Verification出错, %s", err.Error()))
		return nil, err
	}

	return r, nil
}
