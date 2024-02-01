package base

const (
	// 不同类型的字节长度
	DataByteLengthInt64  = 8
	DataByteLengthUint64 = 8
	DataByteLengthString = 4
	// DataByteLengthOffset offset的字节长度，对应的是int64的字节长度
	DataByteLengthOffset = DataByteLengthInt64

	// OffsetNull 空offset对应的值
	OffsetNull = int64(-1)

	RootOffsetValue = int64(0)

	NodeTypeIsLeaf    = 1
	NodeTypeIsNotLeaf = 0

	ValueStringNullValue  = "Null"
	ValueStringErrorValue = "Error"

	// 字段类型
	DBDataTypeInt64  DBDataTypeEnumeration = "int64"
	DBDataTypeString DBDataTypeEnumeration = "string"

	// 模块
	FunctionModelCoreConfig         FunctionModel = "core.config"
	FunctionModelCoreEngine         FunctionModel = "core.engine"
	FunctionModelCoreBPlusTree      FunctionModel = "core.b_plus_tree"
	FunctionModelCoreDataConversion FunctionModel = "core.data_conversion"
	FunctionModelCoreTableSchema    FunctionModel = "core.table_schema"
	FunctionModelCoreDataIO         FunctionModel = "core.data_io"

	// 错误类型
	ErrorTypeSystem ErrorType = "error.system"
	ErrorTypeInput  ErrorType = "error.input"
	ErrorTypeType   ErrorType = "error.type"
	ErrorTypeIO     ErrorType = "error.io"
	ErrorTypeConfig ErrorType = "error.config"

	// 错误代码
	ErrorBaseCodeDefault             = "default"
	ErrorBaseCodeParameterError      = "parameter_error"
	ErrorBaseCodeInnerParameterError = "inner_parameter_error"
	ErrorBaseCodeInnerDataError      = "inner_data_error"
	ErrorBaseCodeInnerTypeError      = "inner_type_error"
	ErrorBaseCodeIOError             = "io"
	ErrorBaseCodeNetworkError        = "network"
	ErrorBaseCodeCoreLogicError      = "code_logic_error"
	ErrorBaseCodeConfigError         = "config"
	ErrorBaseCodeTableSchemaError    = "table_schema"

	DataIOFileTableDataSuffix   = "nedbd"
	DataIOFileTableSchemaSuffix = "nedbs"
)
