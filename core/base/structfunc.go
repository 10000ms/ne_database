package base

func (item *WherePartItem) Validation() bool {
	if item.Args == nil {
		return false
	}
	switch item.Operate {
	case DataComparatorGreater:
		return len(item.Args) == DataComparatorArgsCountGreater
	case DataComparatorGreaterAndEqual:
		return len(item.Args) == DataComparatorArgsCountGreaterAndEqual
	case DataComparatorEqual:
		return len(item.Args) == DataComparatorArgsCountEqual
	case DataComparatorNotEqual:
		return len(item.Args) == DataComparatorArgsCountNotEqual
	case DataComparatorLess:
		return len(item.Args) == DataComparatorArgsCountLess
	case DataComparatorLessAndEqual:
		return len(item.Args) == DataComparatorArgsCountLessAndEqual
	case DataComparatorIn:
		return len(item.Args) > 0
	case DataComparatorNotIn:
		return len(item.Args) > 0
	case DataComparatorBetween:
		return len(item.Args) == DataComparatorArgsCountBetween
	case DataComparatorLike:
		return len(item.Args) == DataComparatorArgsCountLike
	case DataComparatorILike:
		return len(item.Args) == DataComparatorArgsCountILike
	case DataComparatorIsNull:
		return len(item.Args) == DataComparatorArgsCountIsNull
	case DataComparatorIsNotNull:
		return len(item.Args) == DataComparatorArgsCountIsNotNull
	default:
		return false
	}
}
