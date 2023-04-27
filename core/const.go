package core

const (
	// 不同类型的字节长度
	DataByteLengthInt64  = 8
	DataByteLengthString = 4
	// DataByteLengthOffset offset的字节长度，对应的是int64的字节长度
	DataByteLengthOffset = DataByteLengthInt64

	// OffsetNull 空offset对应的值
	OffsetNull = int64(-1)

	// 模块
	FunctionModelCoreBPlusTree      FunctionModel = "core.b_plus_tree"
	FunctionModelCoreDataConversion FunctionModel = "core.data_conversion"

	// 错误类型
	ErrorTypeSystem ErrorType = "error.system"
	ErrorTypeInput  ErrorType = "error.input"

	// 错误代码
	ErrorBaseCodeDefault             = "0001"
	ErrorBaseCodeInnerParameterError = "inner_parameter_error"
)
