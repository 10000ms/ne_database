package tableSchema

type DataTypeEnumeration int

const (
	DataTypeInt64 DataTypeEnumeration = iota
	DataTypeString
)

type MetaType interface {
	GetType() DataTypeEnumeration
}

type Int64Type struct {
}

func (t *Int64Type) GetType() DataTypeEnumeration {
	return DataTypeInt64
}

type StringType struct {
}

func (t *StringType) GetType() DataTypeEnumeration {
	return DataTypeString
}
