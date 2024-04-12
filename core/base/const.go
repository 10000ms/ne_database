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
	DBDataTypeBigInt DBDataTypeEnumeration = "bigint"
	DBDataTypeChar   DBDataTypeEnumeration = "char"

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

	// 文件后缀
	DataIOFileTableDataSuffix   = "nedb"
	DataIOFileTableSchemaSuffix = "neds"

	// 数据储存类型
	StorageTypeFile   = "file"
	StorageTypeMemory = "memory"

	// 比较符
	DataComparatorGreater         DataComparator = "greater"
	DataComparatorGreaterAndEqual DataComparator = "greater_and_equal"
	DataComparatorEqual           DataComparator = "equal"
	DataComparatorNotEqual        DataComparator = "not_equal"
	DataComparatorLess            DataComparator = "less"
	DataComparatorLessAndEqual    DataComparator = "less_and_equal"
	DataComparatorIn              DataComparator = "in"
	DataComparatorNotIn           DataComparator = "not_in"
	DataComparatorBetween         DataComparator = "between"
	DataComparatorLike            DataComparator = "like"
	DataComparatorILike           DataComparator = "ilike"
	DataComparatorIsNull          DataComparator = "is_null"
	DataComparatorIsNotNull       DataComparator = "is_not_null"

	// 比较符支持的参数数量
	DataComparatorArgsCountGreater         = 1
	DataComparatorArgsCountGreaterAndEqual = 1
	DataComparatorArgsCountEqual           = 1
	DataComparatorArgsCountNotEqual        = 1
	DataComparatorArgsCountLess            = 1
	DataComparatorArgsCountLessAndEqual    = 1
	DataComparatorArgsCountIn              = -1
	DataComparatorArgsCountNotIn           = -1
	DataComparatorArgsCountBetween         = 2
	DataComparatorArgsCountLike            = 1
	DataComparatorArgsCountILike           = 1
	DataComparatorArgsCountIsNull          = 0
	DataComparatorArgsCountIsNotNull       = 0
)
