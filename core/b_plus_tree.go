package core

import (
	"encoding/json"
	"fmt"

	"ne_database/core/base"
	"ne_database/core/data_io"
	tableSchema "ne_database/core/table_schema"
	"ne_database/utils"
	"ne_database/utils/list"
	"ne_database/utils/set"
)

type ValueInfo struct {
	Value []byte `json:"value"` // 具体值
}

// BPlusTree B+树结构体
type BPlusTree struct {
	Root        *BPlusTreeNode             // 根节点
	TableInfo   *tableSchema.TableMetaInfo // B+树对应的表信息
	LeafOrder   int                        // 叶子节点的B+树的阶数
	IndexOrder  int                        // 非叶子节点的B+树的阶数
	DataManager data_io.IOManager          // 资源文件的获取方法
}

type BPlusTreeNode struct {
	IsLeaf           bool                    `json:"is_leaf"`            // 是否为叶子节点
	KeysValueList    []*ValueInfo            `json:"-"`                  // key的index
	KeysOffsetList   []int64                 `json:"keys_offset_list"`   // index对应的子节点的offset列表，长度比KeysValueList +1，最后一个是尾部的offset
	DataValues       []map[string]*ValueInfo `json:"-"`                  // 值列表: map[值名]值
	Offset           int64                   `json:"offset"`             // 该节点在硬盘文件中的偏移量，也是该节点的id
	BeforeNodeOffset int64                   `json:"before_node_offset"` // 该节点相连的前一个结点的偏移量
	AfterNodeOffset  int64                   `json:"after_node_offset"`  // 该节点相连的后一个结点的偏移量
}

type noLeafNodeByteDataReadLoopData struct {
	Offset            int64      // offset
	PrimaryKey        *ValueInfo // 主键信息
	OffsetSuccess     bool       // offset获取是否成功
	PrimaryKeySuccess bool       // 主键信息获取是否成功
}

type leafNodeByteDataReadLoopData struct {
	PrimaryKey        *ValueInfo            // 主键信息
	Value             map[string]*ValueInfo // 具体值信息
	PrimaryKeySuccess bool                  // 主键信息获取是否成功
	ValueSuccess      bool                  // 具体值信息获取是否成功
}

type BPlusTreeNodeJSON struct {
	BPlusTreeNode
	KeysStringValue  []string            `json:"keys_value"`  // key的index
	DataStringValues []map[string]string `json:"data_values"` // 值列表: map[值名]值
}

// BPlusTreeJSON B+树 JSON中间结构体
type BPlusTreeJSON struct {
	Root         *BPlusTreeNodeJSON   `json:"root_node"`   // 根节点
	ValueNode    []*BPlusTreeNodeJSON `json:"value_node"`  // 值节点
	RawTableInfo interface{}          `json:"table_info"`  // B+树对应的表信息
	LeafOrder    int                  `json:"leaf_order"`  // 叶子节点的B+树的阶数
	IndexOrder   int                  `json:"index_order"` // 非叶子节点的B+树的阶数
}

type ParentInfo struct {
	LeftParent  int64 `json:"left_parent"`
	OnlyParent  int64 `json:"only_parent"`
	RightParent int64 `json:"right_parent"`
}

func (n *BPlusTreeNodeJSON) JSONTypeToOriginalType() *BPlusTreeNode {
	return &BPlusTreeNode{
		IsLeaf:           n.IsLeaf,
		KeysValueList:    n.KeysValueList,
		KeysOffsetList:   n.KeysOffsetList,
		DataValues:       n.DataValues,
		Offset:           n.Offset,
		BeforeNodeOffset: n.BeforeNodeOffset,
		AfterNodeOffset:  n.AfterNodeOffset,
	}
}

func (n *BPlusTreeNodeJSON) GetValueAndKeyInfo(tableInfo *tableSchema.TableMetaInfo) base.StandardError {
	if n.KeysStringValue != nil {
		n.KeysValueList = make([]*ValueInfo, 0)
		toByteFunc := tableInfo.PrimaryKeyFieldInfo.FieldType.StringToByte
		for _, stringValue := range n.KeysStringValue {
			byteValue, err := toByteFunc(stringValue)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)("[GetValueAndKeyInfo] 获取key的byte值错误")
				return err
			}
			n.KeysValueList = append(n.KeysValueList, &ValueInfo{
				Value: byteValue,
			})
		}
	}
	if n.DataStringValues != nil {
		n.DataValues = make([]map[string]*ValueInfo, 0)
		valueFieldInfoMap, err := tableInfo.ValueFieldInfoMap()
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[GetValueAndKeyInfo] 获取 key info map 错误: %s", err.Error()))
			return err
		}
		n.DataValues = make([]map[string]*ValueInfo, 0)
		for _, row := range n.DataStringValues {
			rowValue := make(map[string]*ValueInfo, 0)
			if row != nil && len(row) != 0 {
				// 表声明中有的每一个值都应该有信息
				if len(row) != len(valueFieldInfoMap) {
					errMsg := fmt.Sprintf("值数量和表声明中的值数量不对！")
					utils.LogError(fmt.Sprintf("[GetValueAndKeyInfo] %s", errMsg))
					return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
				}
				for key, stringValue := range row {
					if valueInfo, ok := valueFieldInfoMap[key]; ok {
						toByteFunc := valueInfo.FieldType.StringToByte
						byteValue, err := toByteFunc(stringValue)
						if err != nil {
							utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[GetValueAndKeyInfo] 获取值<%s>的byte值错误", key))
							return err
						}
						rowValue[key] = &ValueInfo{
							Value: byteValue,
						}
					} else {
						errMsg := fmt.Sprintf("值名称: <%s> 没有出现在表配置当中", key)
						utils.LogError(fmt.Sprintf("[GetValueAndKeyInfo] %s", errMsg))
						return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
					}
				}
			} else {
				errMsg := "值内容为空"
				utils.LogError(fmt.Sprintf("[GetValueAndKeyInfo] %s", errMsg))
				return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
			}
			n.DataValues = append(n.DataValues, rowValue)
		}
	}
	return nil
}

func (n *BPlusTreeNodeJSON) GetValueAndKeyStringValue(tableInfo *tableSchema.TableMetaInfo) base.StandardError {
	n.KeysStringValue = make([]string, 0)
	n.DataStringValues = make([]map[string]string, 0)

	pkKeyInfo := tableInfo.PrimaryKeyFieldInfo.FieldType
	for _, v := range n.KeysValueList {
		n.KeysStringValue = append(n.KeysStringValue, pkKeyInfo.StringValue(v.Value))
	}
	valueKeyInfoMap, err := tableInfo.ValueFieldInfoMap()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[GetValueAndKeyStringValue] tableInfo.ValueFieldInfoMap 错误: %s", err.Error()))
	}
	for _, row := range n.DataValues {
		d := make(map[string]string, 0)
		for name, v := range row {
			if valueKeyInfo, ok := valueKeyInfoMap[name]; ok {
				d[name] = valueKeyInfo.FieldType.StringValue(valueKeyInfo.FieldType.TrimRaw(v.Value))
			} else {
				utils.LogError(fmt.Sprintf("[GetValueAndKeyStringValue] 未知 value name: %s", name))
			}
		}
		n.DataStringValues = append(n.DataStringValues, d)
	}
	return nil
}

// getNoLeafNodeByteDataReadLoopData
// 最后一个offset也需要占用一个完整元素的位置
func getNoLeafNodeByteDataReadLoopData(data []byte, loopTime int, primaryKeyInfo *tableSchema.FieldInfo) (*noLeafNodeByteDataReadLoopData, base.StandardError) {
	var (
		r   = noLeafNodeByteDataReadLoopData{}
		err error
	)
	if primaryKeyInfo == nil {
		errMsg := "传入的 primaryKeyInfo 为空"
		utils.LogError("[getNoLeafNodeByteDataReadLoopData] " + errMsg)
		return &r, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
	}
	var (
		loopLength = primaryKeyInfo.Length + base.DataByteLengthOffset
		startIndex = loopLength * loopTime
	)

	if len(data) < (startIndex + loopLength) {
		// 判断基础的长度
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[getNoLeafNodeByteDataReadLoopData] 长度不够完成这轮解析，返回空")
		return &r, nil
	}
	offsetByte := data[startIndex : startIndex+base.DataByteLengthOffset]
	r.Offset, err = base.ByteListToInt64(offsetByte)
	if err != nil {
		utils.LogError("[getNoLeafNodeByteDataReadLoopData] 传入的 primaryKeyInfo 错误 ", err.Error())
		return &r, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerParameterError, err)
	} else {
		r.OffsetSuccess = true
	}

	fieldValue := data[startIndex+base.DataByteLengthOffset : startIndex+base.DataByteLengthOffset+primaryKeyInfo.Length]
	fieldType := primaryKeyInfo.FieldType
	r.PrimaryKeySuccess = true
	r.PrimaryKey = &ValueInfo{
		Value: fieldType.TrimRaw(fieldValue),
	}
	utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[getNoLeafNodeByteDataReadLoopData] 全部解析完成，返回 ", utils.ToJSON(r))
	return &r, nil
}

// getLeafNodeByteDataReadLoopData
func getLeafNodeByteDataReadLoopData(data []byte, loopTime int, primaryKeyInfo *tableSchema.FieldInfo, valueInfo []*tableSchema.FieldInfo) (*leafNodeByteDataReadLoopData, base.StandardError) {
	var (
		r          = leafNodeByteDataReadLoopData{}
		loopLength int
		startIndex int
		valueIndex int
	)
	// 先进行合法性检查
	if primaryKeyInfo == nil || valueInfo == nil || len(valueInfo) == 0 {
		errMsg := "传入的 primaryKeyInfo / valueInfo (整体) / valueInfo 为空"
		utils.LogError("[getLeafNodeByteDataReadLoopData] " + errMsg)
		return &r, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
	}
	// 1. 计算长度, 开始的位置
	loopLength += primaryKeyInfo.Length
	for _, v := range valueInfo {
		if v == nil {
			errMsg := "传入的 valueInfo (其一) 为空"
			utils.LogError("[getLeafNodeByteDataReadLoopData] " + errMsg)
			return &r, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
		}
		loopLength += v.Length
	}
	startIndex = loopLength * loopTime
	// 1.1 校验长度合法
	if len(data) < (startIndex + loopLength) {
		// 判断基础的长度
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[getLeafNodeByteDataReadLoopData] 长度不够完成这轮解析，返回空")
		return &r, nil
	}
	// 2. 先获取主键信息
	pkValue := data[startIndex : startIndex+primaryKeyInfo.Length]
	pkType := primaryKeyInfo.FieldType
	r.PrimaryKeySuccess = true
	r.PrimaryKey = &ValueInfo{
		Value: pkType.TrimRaw(pkValue),
	}
	// 3. 获取各个值的信息
	valueIndex += primaryKeyInfo.Length
	r.Value = make(map[string]*ValueInfo, 0)
	for _, v := range valueInfo {
		r.Value[v.Name] = &ValueInfo{
			Value: v.FieldType.TrimRaw(data[startIndex+valueIndex : startIndex+valueIndex+v.Length]),
		}
		valueIndex += v.Length
	}
	r.ValueSuccess = true
	utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[getLeafNodeByteDataReadLoopData] 全部解析完成，返回 ", utils.ToJSON(r))
	return &r, nil
}

func (tree *BPlusTree) OffsetLoadNode(offset int64) (*BPlusTreeNode, base.StandardError) {
	rm := tree.DataManager
	nodeData, er := rm.Reader(offset)
	if er != nil {
		utils.LogError("[BPlusTreeNode OffsetToNode Reader] 读取数据错误 " + er.Error())
		return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, er)
	}
	if nodeData == nil || len(nodeData) == 0 {
		errMsg := fmt.Sprintf("offset<%d>读取不到数据", offset)
		utils.LogError("[BPlusTree OffsetLoadNode] " + errMsg)
		return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
	}
	node := &BPlusTreeNode{}
	err := node.LoadByteData(offset, tree.TableInfo, nodeData)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[OffsetToNode] Node LoadByteData 出错: %s", err.Error()))
		return nil, err
	}
	return node, nil
}

// LoadByteData 从[]byte数据中加载节点结构体
func (node *BPlusTreeNode) LoadByteData(offset int64, tableInfo *tableSchema.TableMetaInfo, data []byte) base.StandardError {
	var (
		err base.StandardError
	)
	if data == nil || len(data) == 0 {
		errMsg := fmt.Sprintf("offset<%d>输入数据为空", offset)
		utils.LogError("[BPlusTreeNode LoadByteData] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
	}
	node.Offset = offset
	if len(data) != tableInfo.PageSize {
		errMsg := "输入数据长度不对"
		utils.LogError("[BPlusTreeNode LoadByteData] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
	}

	// 1. 加载这个节点的相邻两个节点的偏移量(offset)
	startOffset := base.DataByteLengthOffset
	endOffset := len(data) - base.DataByteLengthOffset
	node.BeforeNodeOffset, err = base.ByteListToInt64(data[:startOffset])
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] ByteListToInt64 出错, %s", err))
		return err
	}
	node.AfterNodeOffset, err = base.ByteListToInt64(data[endOffset:])
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] ByteListToInt64 出错, %s", err))
		return err
	}

	// 2. 加载判断是否是叶子结点
	if data[startOffset] == base.NodeTypeIsLeaf {
		node.IsLeaf = true
	} else {
		node.IsLeaf = false
	}

	// 3. 加载 node value length
	startOffset += 1
	nodeValueLength, err := base.ByteListToInt64(data[startOffset : startOffset+base.DataByteLengthOffset])
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] ByteListToInt64 出错, %s", err))
		return err
	}
	utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] nodeValueLength: <%d>", nodeValueLength))
	startOffset += base.DataByteLengthOffset
	nodeValueLengthInt := int(nodeValueLength)

	// 3. 加载这个节点的实际数据
	data = data[startOffset:endOffset]
	// 循环次数
	if !node.IsLeaf {
		node.KeysOffsetList = make([]int64, 0)
		node.KeysValueList = make([]*ValueInfo, 0)
		for i := 0; i < nodeValueLengthInt; i++ {
			// 运行数据
			loopData, err := getNoLeafNodeByteDataReadLoopData(data, i, tableInfo.PrimaryKeyFieldInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] getNoLeafNodeByteDataReadLoopData 出错, loopTime: <%d>", i))
				return err
			}
			if loopData.OffsetSuccess == false {
				errMsg := "输入数据长度和声明的不一致"
				utils.LogError("[BPlusTreeNode LoadByteData] " + errMsg)
				return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
			}
			node.KeysOffsetList = append(node.KeysOffsetList, loopData.Offset)
			if i != nodeValueLengthInt-1 {
				node.KeysValueList = append(node.KeysValueList, loopData.PrimaryKey)
			}
		}
	} else {
		node.KeysValueList = make([]*ValueInfo, 0)
		node.DataValues = make([]map[string]*ValueInfo, 0)
		for i := 0; i < nodeValueLengthInt; i++ {
			// 运行数据
			loopData, err := getLeafNodeByteDataReadLoopData(data, i, tableInfo.PrimaryKeyFieldInfo, tableInfo.ValueFieldInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] getLeafNodeByteDataReadLoopData 出错, loopTime: <%d>", i))
				return err
			}
			if loopData.PrimaryKeySuccess == false || loopData.ValueSuccess == false {
				errMsg := "PrimaryKey 或者 Value 获取失败"
				utils.LogError("[BPlusTreeNode LoadByteData] " + errMsg)
				return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
			}
			node.KeysValueList = append(node.KeysValueList, loopData.PrimaryKey)
			node.DataValues = append(node.DataValues, loopData.Value)
		}
	}
	return nil
}

// NodeByteDataLength 判断一个结点转化成为byte数据的长度 TODO: 目前这个方法意义不能，没有使用场景
func (node *BPlusTreeNode) NodeByteDataLength(tree *BPlusTree) int {
	// 基础长度，一个是判断是否是为leaf结点的位，两个是前后相连偏移位
	baseLength := 1 + base.DataByteLengthOffset + base.DataByteLengthOffset
	baseLength += len(node.KeysValueList) * tree.TableInfo.PrimaryKeyFieldInfo.Length
	if node.IsLeaf {
		baseLength += len(node.KeysOffsetList) * base.DataByteLengthOffset
	} else {
		valueLength := 0
		for _, valueInfo := range tree.TableInfo.ValueFieldInfo {
			valueLength += valueInfo.Length
		}
		baseLength += len(node.DataValues) * valueLength
	}
	return baseLength
}

func (node *BPlusTreeNode) NodeToByteData(tableInfo *tableSchema.TableMetaInfo) ([]byte, base.StandardError) {
	var (
		d   = make([]byte, 0)
		err base.StandardError
	)

	// 1. 取前一个结点的偏移量
	beforeNodeByte, err := base.Int64ToByteList(node.BeforeNodeOffset)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 取前一个结点的偏移量出错"))
		return nil, err
	}
	d = append(d, beforeNodeByte...)

	// 2. 取is_leaf
	if node.IsLeaf {
		d = append(d, base.NodeTypeIsLeaf)
	} else {
		d = append(d, base.NodeTypeIsNotLeaf)
	}

	// 3. 取总共长度，以pk长度为准（非叶子结点需要+1）
	l := int64(len(node.KeysValueList))
	if !node.IsLeaf && len(node.KeysValueList) > 0 {
		l += 1
	}
	nodeValueLengthByte, err := base.Int64ToByteList(l)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 结点长度错误: %s", err.Error()))
		return nil, err
	}
	d = append(d, nodeValueLengthByte...)

	// 4. 取内容数据
	if !node.IsLeaf {
		if len(node.KeysOffsetList)-1 != len(node.KeysValueList) && (len(node.KeysOffsetList) != 0 && len(node.KeysValueList) != 0) {
			errMsg := fmt.Sprintf("offset<%d>非法非叶子结点，长度不对", node.Offset)
			utils.LogError("[NodeToByteData] " + errMsg)
			return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
		}
		lengthPaddingFunc := tableInfo.PrimaryKeyFieldInfo.FieldType.LengthPadding
		keyValueLength := tableInfo.PrimaryKeyFieldInfo.Length
		for i := 0; i < len(node.KeysValueList); i++ {
			offsetByte, err := base.Int64ToByteList(node.KeysOffsetList[i])
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 取offsetByte出错"))
				return nil, err
			}
			d = append(d, offsetByte...)
			keyValueByte, err := lengthPaddingFunc(node.KeysValueList[i].Value, keyValueLength)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] keyValueByte.lengthPaddingFunc 出错, %s", err.Error()))
				return nil, err
			}
			d = append(d, keyValueByte...)
		}
		if len(node.KeysValueList) > 0 {
			lastOffsetByte, err := base.Int64ToByteList(node.KeysOffsetList[len(node.KeysOffsetList)-1])
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 取lastOffsetByte出错"))
				return nil, err
			}
			d = append(d, lastOffsetByte...)
		}
	} else {
		if len(node.DataValues) != len(node.KeysValueList) {
			errMsg := "非法叶子结点，长度不对"
			utils.LogError("[NodeToByteData] " + errMsg)
			return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
		}
		lengthPaddingFunc := tableInfo.PrimaryKeyFieldInfo.FieldType.LengthPadding
		keyValueLength := tableInfo.PrimaryKeyFieldInfo.Length
		valueFieldInfoMap, err := tableInfo.ValueFieldInfoMap()
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] tableInfo.ValueFieldInfoMap 出错, %s", err.Error()))
			return nil, err
		}
		for i := 0; i < len(node.KeysValueList); i++ {
			if node.DataValues[i] != nil && len(node.DataValues[i]) != len(tableInfo.ValueFieldInfo) {
				errMsg := "非法叶子结点，值为空或值内容不足"
				utils.LogError("[NodeToByteData] " + errMsg)
				return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
			}
			keyValueByte, err := lengthPaddingFunc(node.KeysValueList[i].Value, keyValueLength)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] keyValueByte.lengthPaddingFunc 出错, %s", err.Error()))
				return nil, err
			}
			d = append(d, keyValueByte...)
			for _, valueFieldInfo := range tableInfo.ValueFieldInfo {
				nodeValue, ok := node.DataValues[i][valueFieldInfo.Name]
				if !ok {
					errMsg := fmt.Sprintf("非法叶子结点，<%s>值不存在", valueFieldInfo.Name)
					utils.LogError("[NodeToByteData] " + errMsg)
					return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
				}
				valueLengthPaddingFunc := valueFieldInfoMap[valueFieldInfo.Name].FieldType.LengthPadding
				valueLength := valueFieldInfoMap[valueFieldInfo.Name].Length
				valueByte, err := valueLengthPaddingFunc(nodeValue.Value, valueLength)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] keyValueByte.lengthPaddingFunc 出错, %s", err.Error()))
					return nil, err
				}
				d = append(d, valueByte...)
			}
		}
	}

	// 5. 补齐中间空余部分
	if tableInfo.PageSize < len(d)-base.DataByteLengthOffset {
		errMsg := "结点长度超长"
		utils.LogError("[NodeToByteData] " + errMsg)
		return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
	}
	d = append(d, make([]uint8, tableInfo.PageSize-len(d)-base.DataByteLengthOffset)...)

	// 6. 取后一个结点的偏移量
	afterNodeByte, err := base.Int64ToByteList(node.AfterNodeOffset)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 取后一个结点的偏移量出错"))
		return nil, err
	}
	d = append(d, afterNodeByte...)
	return d, nil
}

func (tree *BPlusTree) LoadAllNode() (map[int64]*BPlusTreeNode, base.StandardError) {
	allNode := make(map[int64]*BPlusTreeNode, 0)

	// 先加入根节点
	allNode[0] = tree.Root

	// 遍历获取其他节点并加入
	waitHandleList := make([]int64, 0)
	for _, childOffset := range tree.Root.KeysOffsetList {
		waitHandleList = append(waitHandleList, childOffset)
	}
	for len(waitHandleList) > 0 {
		offset := waitHandleList[0]
		waitHandleList = waitHandleList[1:]
		// 空 offset 跳过
		if offset == base.OffsetNull {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("空的 offsets, 跳过"))
			continue
		}
		nodeByte, err := tree.DataManager.Reader(offset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[LoadAllNode.DataManager.Reader]错误: %s", err.Error()))
			return nil, err
		}
		node := BPlusTreeNode{}
		err = node.LoadByteData(offset, tree.TableInfo, nodeByte)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[LoadAllNode.node.LoadByteData]错误: %s", err.Error()))
			return nil, err
		}
		allNode[offset] = &node
		// 如果不是叶子节点，需要加入它的KeysOffsetList
		if !node.IsLeaf {
			for _, cOffset := range node.KeysOffsetList {
				waitHandleList = append(waitHandleList, cOffset)
			}
		}
	}
	return allNode, nil
}

// Insert 插入键值对
func (tree *BPlusTree) Insert(key []byte, value [][]byte) base.StandardError {
	var (
		curNode         = tree.Root // 当前 node
		parentOffsetMap = make(map[int64]*ParentInfo)
		waitWriterMap   = make(map[int64][]byte)
		newAssignPage   = make([]int64, 0)
		err             base.StandardError
	)

	if key == nil || len(key) == 0 {
		errMsg := fmt.Sprintf("key 数据为空")
		utils.LogError(fmt.Sprintf("[BPlusTree.Insert] %s", errMsg))
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}

	if value == nil || len(value) != len(tree.TableInfo.ValueFieldInfo) {
		errMsg := fmt.Sprintf("value 数据为空或数量不对")
		utils.LogError(fmt.Sprintf("[BPlusTree.Insert] %s", errMsg))
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}

	parentOffsetMap, err = tree.NodeParentMap()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert.NodeParentMap]错误: %s", err.Error()))
		return err
	}

	// 1. 查找插入位置
	for !curNode.IsLeaf {
		index := 0
		for ; index < len(curNode.KeysValueList); index++ {
			greater, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Greater(curNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] Greater 错误: %s", err.Error()))
				return err
			}
			if greater {
				break
			}
			equal, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Equal(curNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] Equal 错误: %s", err.Error()))
				return err
			}
			if equal {
				break
			}
		}
		nextOffset := curNode.KeysOffsetList[index]
		curNode, err = tree.OffsetLoadNode(nextOffset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] tree.OffsetLoadNode 错误: %s", err.Error()))
			return err
		}
	}
	// 2. 向叶子节点插入键值对
	index := 0
	for ; index < len(curNode.KeysValueList); index++ {
		greater, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Greater(curNode.KeysValueList[index].Value, key)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[BPlusTree.Insert] Greater 错误: %s", err.Error()))
			return err
		}
		if greater {
			break
		}
		equal, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Equal(curNode.KeysValueList[index].Value, key)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] Equal 错误: %s", err.Error()))
			return err
		}
		if equal {
			break
		}
	}

	curNode.KeysValueList = append(curNode.KeysValueList, nil)
	curNode.DataValues = append(curNode.DataValues, nil)
	copy(curNode.KeysValueList[index+1:], curNode.KeysValueList[index:])
	copy(curNode.DataValues[index+1:], curNode.DataValues[index:])
	keyTrimFunc := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.TrimRaw
	curNode.KeysValueList[index] = &ValueInfo{
		Value: keyTrimFunc(key),
	}
	dataValue := make(map[string]*ValueInfo, 0)
	for i, fieldInfo := range tree.TableInfo.ValueFieldInfo {
		valueTrimFunc := tree.TableInfo.ValueFieldInfo[i].FieldType.TrimRaw
		dataValue[fieldInfo.Name] = &ValueInfo{
			Value: valueTrimFunc(value[i]),
		}
	}
	curNode.DataValues[index] = dataValue

	// 2.1 更新值
	curNodeByte, err := curNode.NodeToByteData(tree.TableInfo)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] beforeNode.NodeToByteData 错误: %s", err.Error()))
		return err
	}
	waitWriterMap[curNode.Offset] = curNodeByte

	// 3. 如果该叶子节点满了，进行分裂操作
	for (!curNode.IsLeaf && len(curNode.KeysOffsetList) == tree.IndexOrder) || (curNode.IsLeaf && len(curNode.KeysValueList) == tree.LeafOrder) {
		// 3.1 分裂叶子节点
		var splitIndex int
		if curNode.IsLeaf {
			splitIndex = tree.LeafOrder / 2
		} else {
			splitIndex = tree.IndexOrder / 2
		}

		nextEmptyOffset, err := tree.DataManager.AssignEmptyPage()
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] GetNextEmptyOffset 错误: %s", err.Error()))
			return err
		}
		newAssignPage = append(newAssignPage, nextEmptyOffset)

		newNode := &BPlusTreeNode{
			IsLeaf:           curNode.IsLeaf,
			Offset:           nextEmptyOffset,
			KeysValueList:    make([]*ValueInfo, 0),
			BeforeNodeOffset: curNode.Offset,
			AfterNodeOffset:  curNode.AfterNodeOffset,
		}

		// 新 node 拿后面的部分

		lastCurNodeAfterNodeOffset := curNode.AfterNodeOffset
		curNode.AfterNodeOffset = newNode.Offset

		newNode.KeysValueList = append(newNode.KeysValueList, curNode.KeysValueList[splitIndex:]...)
		curNode.KeysValueList = curNode.KeysValueList[:splitIndex]

		if curNode.IsLeaf {
			newNode.DataValues = make([]map[string]*ValueInfo, 0)
			newNode.DataValues = append(newNode.DataValues, curNode.DataValues[splitIndex:]...)
			curNode.DataValues = curNode.DataValues[:splitIndex]
		} else {
			newNode.KeysOffsetList = make([]int64, 0)
			newNode.KeysOffsetList = append(newNode.KeysOffsetList, curNode.KeysOffsetList[splitIndex:]...)
			curNode.KeysOffsetList = curNode.KeysOffsetList[:splitIndex+1]
		}

		// 记录 curNode
		curNodeByte, err := curNode.NodeToByteData(tree.TableInfo)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] beforeNode.NodeToByteData 错误: %s", err.Error()))
			return err
		}
		waitWriterMap[curNode.Offset] = curNodeByte

		// 记录 newNode
		newNodeByte, err := newNode.NodeToByteData(tree.TableInfo)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] beforeNode.NodeToByteData 错误: %s", err.Error()))
			return err
		}
		waitWriterMap[newNode.Offset] = newNodeByte

		if lastCurNodeAfterNodeOffset != base.OffsetNull {
			// curNode 的 afterNode 需要更新 BeforeNodeOffset
			afterNode, err := tree.OffsetLoadNode(lastCurNodeAfterNodeOffset)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] tree.OffsetLoadNode 错误: %s", err.Error()))
				return err
			}
			afterNode.BeforeNodeOffset = newNode.Offset
			afterNodeByte, err := afterNode.NodeToByteData(tree.TableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] beforeNode.NodeToByteData 错误: %s", err.Error()))
				return err
			}
			waitWriterMap[afterNode.Offset] = afterNodeByte
		}

		// 3.2. 更新父节点的键列表和子节点列表
		if curNode.Offset == base.RootOffsetValue {
			// 结点是root

			if curNode.Offset == base.RootOffsetValue && curNode != tree.Root {
				errMsg := fmt.Sprintf("curNode应该为root而实际不为")
				utils.LogError(fmt.Sprintf("[BPlusTree.Insert] %s", errMsg))
				return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeCoreLogicError, fmt.Errorf(errMsg))
			}

			// curNode 需要分配新的 offset 再记录
			nextEmptyOffset, err := tree.DataManager.AssignEmptyPage()
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] GetNextEmptyOffset 错误: %s", err.Error()))
				return err
			}
			newAssignPage = append(newAssignPage, nextEmptyOffset)
			curNode.Offset = nextEmptyOffset
			curNodeByte, err := curNode.NodeToByteData(tree.TableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] beforeNode.NodeToByteData 错误: %s", err.Error()))
				return err
			}
			waitWriterMap[curNode.Offset] = curNodeByte
			// newNode 需要更新 BeforeNodeOffset 记录
			newNode.BeforeNodeOffset = nextEmptyOffset
			newNodeByte, err := newNode.NodeToByteData(tree.TableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] beforeNode.NodeToByteData 错误: %s", err.Error()))
				return err
			}
			waitWriterMap[newNode.Offset] = newNodeByte

			newRoot := &BPlusTreeNode{
				IsLeaf:           false, // 需求分裂 root 的场景下，root 必然不是 leaf node
				Offset:           base.RootOffsetValue,
				BeforeNodeOffset: base.OffsetNull,
				AfterNodeOffset:  base.OffsetNull,
			}
			newRoot.KeysValueList = []*ValueInfo{
				{
					Value: curNode.KeysValueList[len(curNode.KeysValueList)-1].Value,
				},
			}
			newRoot.KeysOffsetList = []int64{curNode.Offset, newNode.Offset}

			tree.Root = newRoot

			// 记录新的 root
			newRootByte, err := newRoot.NodeToByteData(tree.TableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] beforeNode.NodeToByteData 错误: %s", err.Error()))
				return err
			}
			waitWriterMap[newRoot.Offset] = newRootByte

		} else {
			var (
				pOffset int64
				pInfo   *ParentInfo
				ok      bool
			)
			pInfo, ok = parentOffsetMap[curNode.Offset]
			if ok != true || pInfo == nil {
				errMsg := fmt.Sprintf("<%d>节点找不到父节点", curNode.Offset)
				utils.LogError(fmt.Sprintf("[BPlusTree.Insert] %s", errMsg))
				return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
			}
			if pInfo.OnlyParent != base.OffsetNull {
				pOffset = pInfo.OnlyParent
			} else {
				pOffset = pInfo.RightParent
			}
			parentNode, err := tree.OffsetLoadNode(pOffset)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] tree.OffsetLoadNode 错误: %s", err.Error()))
				return err
			}

			// 更新父节点的 KeysValueList 和 KeysOffsetList
			newKey := curNode.KeysValueList[len(curNode.KeysValueList)-1].Value
			index := 0
			for ; index < len(parentNode.KeysValueList); index++ {
				greater, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Greater(parentNode.KeysValueList[index].Value, newKey)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] Greater 错误: %s", err.Error()))
					return err
				}
				if greater {
					break
				}
				equal, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Equal(parentNode.KeysValueList[index].Value, newKey)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] Equal 错误: %s", err.Error()))
					return err
				}
				if equal {
					break
				}
			}
			parentNode.KeysValueList = append(parentNode.KeysValueList, nil)
			parentNode.KeysOffsetList = append(parentNode.KeysOffsetList, 0)
			copy(parentNode.KeysValueList[index+1:], parentNode.KeysValueList[index:])
			copy(parentNode.KeysOffsetList[index+1:], parentNode.KeysOffsetList[index:])
			parentNode.KeysValueList[index] = &ValueInfo{
				Value: newKey,
			}
			parentNode.KeysOffsetList[index+1] = newNode.Offset // 这里分裂出来的结果肯定是更贴近 KeysValueList[index] 的，所以 index 需要 +1
			// 记录 parentNode
			parentNodeByte, err := parentNode.NodeToByteData(tree.TableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] beforeNode.NodeToByteData 错误: %s", err.Error()))
				return err
			}
			waitWriterMap[parentNode.Offset] = parentNodeByte
			if parentNode.Offset == base.RootOffsetValue {
				tree.Root = parentNode
			}

			// 判断父结点是否需要处理
			if (!parentNode.IsLeaf && len(parentNode.KeysOffsetList) == tree.IndexOrder) || (parentNode.IsLeaf && len(parentNode.KeysValueList) == tree.LeafOrder) {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("curNode %d => parentNode %d", curNode.Offset, parentNode.Offset))
				curNode = parentNode
			} else {
				break
			}
		}
	}

	// 4. 更新涉及到的 page 落盘
	// FIXME 写不成功需要考虑整体回滚
	for offset, data := range waitWriterMap {
		success, err := tree.DataManager.Writer(offset, data)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Insert] Writer 错误: %s", err.Error()))
			return err
		}
		if !success {
			errMsg := fmt.Sprintf("写入offset <%d>失败", offset)
			utils.LogError(fmt.Sprintf("[BPlusTree.Insert] %s", errMsg))
			return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
		}

	}
	return nil
}

// Update 更新值
func (tree *BPlusTree) Update(key []byte, values map[string][]byte) base.StandardError {
	var (
		curNode                   = tree.Root // 当前 node
		checkUpdateLeafNodeOffset = set.NewInt64sSet()
		updatedLeafNodeOffset     = set.NewInt64sSet()
		err                       base.StandardError
	)

	if key == nil || len(key) == 0 {
		errMsg := fmt.Sprintf("key 数据为空")
		utils.LogError(fmt.Sprintf("[BPlusTree.Update] %s", errMsg))
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
	}

	if values == nil || len(values) == 0 {
		errMsg := fmt.Sprintf("values 数据为空")
		utils.LogError(fmt.Sprintf("[BPlusTree.Update] %s", errMsg))
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
	}

	// 1. 查找更新位置
	for !curNode.IsLeaf {
		index := 0
		for ; index < len(curNode.KeysValueList); index++ {
			greater, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Greater(curNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] Greater 错误: %s", err.Error()))
				return err
			}
			if greater {
				break
			}
			equal, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Equal(curNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] Equal 错误: %s", err.Error()))
				return err
			}
			if equal {
				break
			}
		}
		nextOffset := curNode.KeysOffsetList[index]
		curNode, err = tree.OffsetLoadNode(nextOffset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] tree.OffsetLoadNode 错误: %s", err.Error()))
			return err
		}
	}

	// 2. 修改对应数据
	checkUpdateLeafNodeOffset.Add(curNode.Offset)
	for len(checkUpdateLeafNodeOffset.Difference(updatedLeafNodeOffset).TotalMember()) > 0 {
		nodeOffset := checkUpdateLeafNodeOffset.Difference(updatedLeafNodeOffset).TotalMember()[0]
		updatedLeafNodeOffset.Add(nodeOffset)

		dNode, err := tree.OffsetLoadNode(nodeOffset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] tree.OffsetLoadNode 错误: %s", err.Error()))
			return err
		}

		hasChange := false
		for index := 0; index < len(dNode.KeysValueList); index++ {
			equal, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Equal(dNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] Equal 错误: %s", err.Error()))
				return err
			}
			if equal {
				hasChange = true

				if index == 0 && dNode.BeforeNodeOffset != base.OffsetNull {
					checkUpdateLeafNodeOffset.Add(dNode.BeforeNodeOffset)
				}
				if index == len(dNode.DataValues)-1 && dNode.AfterNodeOffset != base.OffsetNull {
					checkUpdateLeafNodeOffset.Add(dNode.AfterNodeOffset)
				}

				for valueName, v := range values {
					if v == nil {
						errMsg := fmt.Sprintf("field<%s> 对应 values 内容为nil", valueName)
						utils.LogError(fmt.Sprintf("[BPlusTree.Update] %s", errMsg))
						return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))

					}
					_, ok := dNode.DataValues[index][valueName]
					if !ok {
						errMsg := fmt.Sprintf("field<%s> 不存在", valueName)
						utils.LogError(fmt.Sprintf("[BPlusTree.Update] %s", errMsg))
						return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
					}
					dNode.DataValues[index][valueName].Value = v
				}
			}
			greater, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Greater(dNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] Greater 错误: %s", err.Error()))
				return err
			}
			if greater {
				break
			}
		}

		if hasChange {
			dNodeByte, err := dNode.NodeToByteData(tree.TableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] curNode.NodeToByteData 错误: %s", err.Error()))
				return err
			}
			success, err := tree.DataManager.Writer(dNode.Offset, dNodeByte)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
				return err
			}
			if !success {
				errMsg := fmt.Sprintf("写入offset <%d>失败", dNode.Offset)
				utils.LogError(fmt.Sprintf("[BPlusTree.Update] %s", errMsg))
				return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
			}
		}
	}

	return nil
}

// Delete 删除键值对
func (tree *BPlusTree) Delete(key []byte) base.StandardError {
	var (
		curNode                           = tree.Root // 当前 node
		checkDeleteLeafNodeOffset         = make([]int64, 0)
		needDeleteNodeOffset              = make([]int64, 0)
		checkDeleteLeafNodeOffsetToParent = make([]int64, 0)
		parentOffsetMap                   = make(map[int64]*ParentInfo)
		err                               base.StandardError
	)

	if key == nil || len(key) == 0 {
		errMsg := fmt.Sprintf("key 数据为空")
		utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}

	parentOffsetMap, err = tree.NodeParentMap()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.NodeParentMap]错误: %s", err.Error()))
		return err
	}

	// 1. 查找对应的叶子节点
	for !curNode.IsLeaf {
		index := 0
		for ; index < len(curNode.KeysValueList); index++ {
			greater, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Greater(curNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Greater 错误: %s", err.Error()))
				return err
			}
			if greater {
				break
			}
			equal, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Equal(curNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Equal 错误: %s", err.Error()))
				return err
			}
			if equal {
				break
			}
		}
		nextOffset := curNode.KeysOffsetList[index]
		curNode, err = tree.OffsetLoadNode(nextOffset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] tree.OffsetLoadNode 错误: %s", err.Error()))
			return err
		}
	}

	// 2. 删除键值对
	checkDeleteLeafNodeOffset = append(checkDeleteLeafNodeOffset, curNode.Offset)
	for len(checkDeleteLeafNodeOffset) > 0 {
		dNodeOffset := checkDeleteLeafNodeOffset[0]
		checkDeleteLeafNodeOffset = checkDeleteLeafNodeOffset[1:]

		var (
			pOffset *ParentInfo
			ok      bool
		)
		if dNodeOffset == base.RootOffsetValue {
			pOffset = &ParentInfo{}
			pOffset.OnlyParent = base.OffsetNull
		} else {
			pOffset, ok = parentOffsetMap[dNodeOffset]
			if !ok || pOffset == nil {
				errMsg := fmt.Sprintf("offset<%d> 找不到父offset", pOffset)
				utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
				return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
			}
		}
		dNode, err := tree.OffsetLoadNode(dNodeOffset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
			return err
		}
		remainItem, leftCheck, rightCheck, err := dNode.LeafNodeClear(key, tree.TableInfo)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] curNode.LeafNodeClear 错误: %s", err.Error()))
			return err
		}
		if leftCheck && dNode.Offset != base.RootOffsetValue && dNode.BeforeNodeOffset != base.OffsetNull {
			checkDeleteLeafNodeOffset = append(checkDeleteLeafNodeOffset, dNode.BeforeNodeOffset)
		}
		if rightCheck && dNode.Offset != base.RootOffsetValue && dNode.AfterNodeOffset != base.OffsetNull {
			checkDeleteLeafNodeOffset = append(checkDeleteLeafNodeOffset, dNode.AfterNodeOffset)
		}
		if remainItem == 0 && dNode.Offset != base.RootOffsetValue {
			checkDeleteLeafNodeOffsetToParent = append(checkDeleteLeafNodeOffsetToParent, dNode.Offset)
			needDeleteNodeOffset = append(needDeleteNodeOffset, dNode.Offset)
			// 左右结点的连接信息也需要处理
			if dNode.BeforeNodeOffset != base.OffsetNull {
				beforeNode, err := tree.OffsetLoadNode(dNode.BeforeNodeOffset)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
					return err
				}
				beforeNode.AfterNodeOffset = dNode.AfterNodeOffset
				beforeNodeByte, err := beforeNode.NodeToByteData(tree.TableInfo)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] dNodeByte.NodeToByteData 错误: %s", err.Error()))
					return err
				}
				success, err := tree.DataManager.Writer(beforeNode.Offset, beforeNodeByte)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
					return err
				}
				if !success {
					errMsg := fmt.Sprintf("写入offset <%d>失败", beforeNode.Offset)
					utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
					return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
				}
			}
			if dNode.AfterNodeOffset != base.OffsetNull {
				afterNode, err := tree.OffsetLoadNode(dNode.AfterNodeOffset)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
					return err
				}
				afterNode.BeforeNodeOffset = dNode.BeforeNodeOffset
				afterNodeByte, err := afterNode.NodeToByteData(tree.TableInfo)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] dNodeByte.NodeToByteData 错误: %s", err.Error()))
					return err
				}
				success, err := tree.DataManager.Writer(afterNode.Offset, afterNodeByte)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
					return err
				}
				if !success {
					errMsg := fmt.Sprintf("写入offset <%d>失败", afterNode.Offset)
					utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
					return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
				}
			}
		}
		dNodeByte, err := dNode.NodeToByteData(tree.TableInfo)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] dNodeByte.NodeToByteData 错误: %s", err.Error()))
			return err
		}
		if dNode.Offset == base.RootOffsetValue {
			tree.Root = dNode
		}
		success, err := tree.DataManager.Writer(dNode.Offset, dNodeByte)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
			return err
		}
		if !success {
			errMsg := fmt.Sprintf("写入offset <%d>失败", dNode.Offset)
			utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
			return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
		}
	}

	// 2.1 父结点也需要处理
	for len(checkDeleteLeafNodeOffsetToParent) > 0 {
		childNodeOffset := checkDeleteLeafNodeOffsetToParent[0]
		checkDeleteLeafNodeOffsetToParent = checkDeleteLeafNodeOffsetToParent[1:]

		var (
			pInfo *ParentInfo
			ok    bool
		)

		pInfo, ok = parentOffsetMap[childNodeOffset]
		if !ok || pInfo == nil {
			errMsg := fmt.Sprintf("offset<%d> 找不到父offset", childNodeOffset)
			utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
			return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
		}

		parentOffsetList := make([]int64, 0)

		if pInfo.OnlyParent != base.OffsetNull {
			parentOffsetList = append(parentOffsetList, pInfo.OnlyParent)
		} else {

			parentOffsetList = append(parentOffsetList, pInfo.LeftParent)
			parentOffsetList = append(parentOffsetList, pInfo.RightParent)
		}

		for len(parentOffsetList) > 0 {
			pOffset := parentOffsetList[0]
			parentOffsetList = parentOffsetList[1:]

			dNode, err := tree.OffsetLoadNode(pOffset)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
				return err
			}
			remainItem, hasFirstChange, hasLastChange, err := dNode.IndexNodeClear(childNodeOffset, tree)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] curNode.IndexNodeClear 错误: %s", err.Error()))
				return err
			}
			if remainItem == 0 && dNode.Offset == base.RootOffsetValue {
				var newRootOffset int64
				if hasFirstChange == true {
					newRootOffset = dNode.KeysOffsetList[len(dNode.KeysOffsetList)-1]
				} else {
					newRootOffset = dNode.KeysOffsetList[0]
				}
				err := tree.ChangeRoot(newRootOffset)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.ChangeRoot] 错误: %s", err.Error()))
					return err
				}
				needDeleteNodeOffset = append(needDeleteNodeOffset, newRootOffset)
				// 需要刷新parentOffsetMap
				parentOffsetMap, err = tree.NodeParentMap()
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.NodeParentMap]错误: %s", err.Error()))
					return err
				}
				// 有可能有残留的 newRootOffset
				for _, o := range checkDeleteLeafNodeOffsetToParent {
					newList := make([]int64, 0)
					if o != newRootOffset {
						newList = append(newList, o)
					}
					checkDeleteLeafNodeOffsetToParent = newList

				}
			} else if remainItem == 0 && dNode.Offset != base.RootOffsetValue {
				checkDeleteLeafNodeOffsetToParent = append(checkDeleteLeafNodeOffsetToParent, dNode.Offset)
				needDeleteNodeOffset = append(needDeleteNodeOffset, dNode.Offset)
				// 左右结点的连接信息也需要处理
				if dNode.BeforeNodeOffset != base.OffsetNull {
					beforeNode, err := tree.OffsetLoadNode(dNode.BeforeNodeOffset)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
						return err
					}
					beforeNode.AfterNodeOffset = dNode.AfterNodeOffset
					beforeNodeByte, err := beforeNode.NodeToByteData(tree.TableInfo)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] dNodeByte.NodeToByteData 错误: %s", err.Error()))
						return err
					}
					success, err := tree.DataManager.Writer(beforeNode.Offset, beforeNodeByte)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
						return err
					}
					if !success {
						errMsg := fmt.Sprintf("写入offset <%d>失败", beforeNode.Offset)
						utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
						return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
					}
				}
				if dNode.AfterNodeOffset != base.OffsetNull {
					afterNode, err := tree.OffsetLoadNode(dNode.AfterNodeOffset)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
						return err
					}
					afterNode.AfterNodeOffset = dNode.AfterNodeOffset
					if dNode.BeforeNodeOffset == base.OffsetNull {
						// pass
					} else {
						// 要指向beforeNode的-1个KeysOffsetList
						beforeNode, err := tree.OffsetLoadNode(dNode.BeforeNodeOffset)
						if err != nil {
							utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
							return err
						}
						afterNode.KeysOffsetList[0] = beforeNode.KeysOffsetList[len(beforeNode.KeysOffsetList)-1]
					}
					afterNodeByte, err := afterNode.NodeToByteData(tree.TableInfo)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] dNodeByte.NodeToByteData 错误: %s", err.Error()))
						return err
					}
					success, err := tree.DataManager.Writer(afterNode.Offset, afterNodeByte)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
						return err
					}
					if !success {
						errMsg := fmt.Sprintf("写入offset <%d>失败", afterNode.Offset)
						utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
						return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
					}
				}
			} else {
				dNodeByte, err := dNode.NodeToByteData(tree.TableInfo)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] dNodeByte.NodeToByteData 错误: %s", err.Error()))
					return err
				}
				success, err := tree.DataManager.Writer(dNode.Offset, dNodeByte)
				if err != nil {
					utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
					return err
				}
				if !success {
					errMsg := fmt.Sprintf("写入offset <%d>失败", dNode.Offset)
					utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
					return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
				}
				// 如果首尾删除，需要处理相邻两个结点
				if hasFirstChange && dNode.BeforeNodeOffset != base.OffsetNull {
					beforeNode, err := tree.OffsetLoadNode(dNode.BeforeNodeOffset)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
						return err
					}
					beforeNode.KeysOffsetList[len(beforeNode.KeysOffsetList)-1] = dNode.KeysOffsetList[0]
					beforeNodeByte, err := beforeNode.NodeToByteData(tree.TableInfo)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] dNodeByte.NodeToByteData 错误: %s", err.Error()))
						return err
					}
					success, err := tree.DataManager.Writer(beforeNode.Offset, beforeNodeByte)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
						return err
					}
					if !success {
						errMsg := fmt.Sprintf("写入offset <%d>失败", beforeNode.Offset)
						utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
						return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
					}
				}
				if hasLastChange && dNode.AfterNodeOffset != base.OffsetNull {
					afterNode, err := tree.OffsetLoadNode(dNode.AfterNodeOffset)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete.OffsetLoadNode] 错误: %s", err.Error()))
						return err
					}
					afterNode.KeysOffsetList[0] = dNode.KeysOffsetList[len(dNode.KeysOffsetList)-1]
					afterNodeByte, err := afterNode.NodeToByteData(tree.TableInfo)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] dNodeByte.NodeToByteData 错误: %s", err.Error()))
						return err
					}
					success, err := tree.DataManager.Writer(afterNode.Offset, afterNodeByte)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
						return err
					}
					if !success {
						errMsg := fmt.Sprintf("写入offset <%d>失败", afterNode.Offset)
						utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
						return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
					}
				}
			}
		}
	}

	// 3 结点删除
	for _, offset := range needDeleteNodeOffset {
		success, err := tree.DataManager.Delete(offset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Delete 错误: %s", err.Error()))
			return err
		}
		if !success {
			errMsg := fmt.Sprintf("删除offset <%d>失败", offset)
			utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
			return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
		}
	}

	return nil
}

// SearchEqualKey 查找键对应的值
func (tree *BPlusTree) SearchEqualKey(key []byte) ([][]byte, []map[string][]byte, base.StandardError) {
	var (
		curNode                 = tree.Root // 当前 node
		waitCheckLeafNodeOffset = set.NewInt64sSet()
		checkedLeafNodeOffset   = set.NewInt64sSet()
		retKeyList              = make([][]byte, 0)
		retValueList            = make([]map[string][]byte, 0)
		err                     base.StandardError
	)

	if key == nil || len(key) == 0 {
		errMsg := fmt.Sprintf("key 数据为空")
		utils.LogError(fmt.Sprintf("[BPlusTree.SearchEqualKey] %s", errMsg))
		return nil, nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
	}

	// 1. 查找位置
	for !curNode.IsLeaf {
		index := 0
		for ; index < len(curNode.KeysValueList); index++ {
			greater, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Greater(curNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] Greater 错误: %s", err.Error()))
				return nil, nil, err
			}
			if greater {
				break
			}
			equal, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Equal(curNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] Equal 错误: %s", err.Error()))
				return nil, nil, err
			}
			if equal {
				break
			}
		}
		nextOffset := curNode.KeysOffsetList[index]
		curNode, err = tree.OffsetLoadNode(nextOffset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] tree.OffsetLoadNode 错误: %s", err.Error()))
			return nil, nil, err
		}
	}

	// 2. 获取对应数据
	waitCheckLeafNodeOffset.Add(curNode.Offset)
	for len(waitCheckLeafNodeOffset.Difference(checkedLeafNodeOffset).TotalMember()) > 0 {
		nodeOffset := waitCheckLeafNodeOffset.Difference(checkedLeafNodeOffset).TotalMember()[0]
		checkedLeafNodeOffset.Add(nodeOffset)

		dNode, err := tree.OffsetLoadNode(nodeOffset)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.SearchEqualKey] tree.OffsetLoadNode 错误: %s", err.Error()))
			return nil, nil, err
		}

		for index := 0; index < len(dNode.KeysValueList); index++ {
			equal, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Equal(dNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.SearchEqualKey] Equal 错误: %s", err.Error()))
				return nil, nil, err
			}
			if equal {
				if index == 0 && dNode.BeforeNodeOffset != base.OffsetNull {
					waitCheckLeafNodeOffset.Add(dNode.BeforeNodeOffset)
				}
				if index == len(dNode.DataValues)-1 && dNode.AfterNodeOffset != base.OffsetNull {
					waitCheckLeafNodeOffset.Add(dNode.AfterNodeOffset)
				}

				retKeyList = append(retKeyList, dNode.KeysValueList[index].Value)
				values := make(map[string][]byte)
				for k, v := range dNode.DataValues[index] {
					values[k] = v.Value
				}
				retValueList = append(retValueList, values)
			}
			greater, err := tree.TableInfo.PrimaryKeyFieldInfo.FieldType.Greater(dNode.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Update] Greater 错误: %s", err.Error()))
				return nil, nil, err
			}
			if greater {
				break
			}
		}
	}

	return retKeyList, retValueList, nil
}

func (tree *BPlusTree) ChangeRoot(newRootOffset int64) base.StandardError {
	utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[BPlusTree.ChangeRoot] change to: %d", newRootOffset))
	newRoot, err := tree.OffsetLoadNode(newRootOffset)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.ChangeRoot.OffsetLoadNode] 错误: %s", err.Error()))
		return err
	}
	tree.Root = newRoot
	tree.Root.Offset = base.RootOffsetValue
	tree.Root.BeforeNodeOffset = base.OffsetNull
	tree.Root.AfterNodeOffset = base.OffsetNull
	nodeByte, err := tree.Root.NodeToByteData(tree.TableInfo)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.ChangeRoot] dNodeByte.NodeToByteData 错误: %s", err.Error()))
		return err
	}
	success, err := tree.DataManager.Writer(tree.Root.Offset, nodeByte)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTree.Delete] Writer 错误: %s", err.Error()))
		return err
	}
	if !success {
		errMsg := fmt.Sprintf("写入offset <%d>失败", tree.Root.Offset)
		utils.LogError(fmt.Sprintf("[BPlusTree.Delete] %s", errMsg))
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}
	return nil
}

func (node *BPlusTreeNode) LeafNodeClear(key []byte, tableInfo *tableSchema.TableMetaInfo) (int, bool, bool, base.StandardError) {
	if !node.IsLeaf {
		errMsg := fmt.Sprintf("非leaf结点使用leaf删除")
		utils.LogError(fmt.Sprintf("[BPlusTreeNode.LeafDelete] %s", errMsg))
		return 0, false, false, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerTypeError, fmt.Errorf(errMsg))
	}
	var (
		leftCheck  = false
		rightCheck = false
	)
	index := 0
	for {
		findIndex := len(node.KeysValueList)
		for ; index < len(node.KeysValueList); index++ {
			greater, err := tableInfo.PrimaryKeyFieldInfo.FieldType.Greater(node.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.Delete] Greater 错误: %s", err.Error()))
				return 0, false, false, err
			}
			if greater {
				break
			}
			equal, err := tableInfo.PrimaryKeyFieldInfo.FieldType.Equal(node.KeysValueList[index].Value, key)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.Delete] Equal 错误: %s", err.Error()))
				return 0, false, false, err
			}
			if equal {
				findIndex = index
				break
			}
		}
		if findIndex < len(node.KeysValueList) {
			if findIndex == 0 {
				leftCheck = true
			}
			if findIndex == len(node.KeysValueList)-1 {
				rightCheck = true
			}

			copy(node.KeysValueList[findIndex:], node.KeysValueList[findIndex+1:])
			copy(node.DataValues[findIndex:], node.DataValues[findIndex+1:])
			node.KeysValueList = node.KeysValueList[:len(node.KeysValueList)-1]
			node.DataValues = node.DataValues[:len(node.DataValues)-1]
		} else {
			break
		}
	}
	return len(node.KeysValueList), leftCheck, rightCheck, nil
}

func (node *BPlusTreeNode) IndexNodeClear(offset int64, tree *BPlusTree) (int, bool, bool, base.StandardError) {
	if node.IsLeaf {
		errMsg := fmt.Sprintf("leaf结点使用非leaf删除")
		utils.LogError(fmt.Sprintf("[BPlusTreeNode.NoLeafDelete] %s", errMsg))
		return 0, false, false, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerTypeError, fmt.Errorf(errMsg))
	}
	if offset == base.OffsetNull {
		errMsg := fmt.Sprintf("删除空offset")
		utils.LogError(fmt.Sprintf("[BPlusTreeNode.NoLeafDelete] %s", errMsg))
		return 0, false, false, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
	}
	index := 0
	hasFirstChange := false
	hasLastChange := false
	deleteKeyFunc := func(i int) {
		copy(node.KeysValueList[i:], node.KeysValueList[i+1:])
		copy(node.KeysOffsetList[i:], node.KeysOffsetList[i+1:])
		node.KeysValueList = node.KeysValueList[:len(node.KeysValueList)-1]
		node.KeysOffsetList = node.KeysOffsetList[:len(node.KeysOffsetList)-1]
	}
	deleteNodeFunc := func() {
		hasFirstChange = true
		hasLastChange = true
		node.KeysValueList = node.KeysValueList[:0]
		node.KeysOffsetList = node.KeysOffsetList[:0]
	}
	for ; index < len(node.KeysOffsetList); index++ {
		if offset == node.KeysOffsetList[index] {
			break
		}
	}
	if index < len(node.KeysOffsetList) {
		if index == 0 || index == len(node.KeysOffsetList)-1 {
			if index == 0 {
				hasFirstChange = true
				if len(node.KeysValueList) > 1 {
					deleteKeyFunc(index)
				} else if node.Offset == base.RootOffsetValue {
					// root 节点需要特殊处理
					hasFirstChange = true
					return 0, hasFirstChange, hasLastChange, nil
				} else if node.BeforeNodeOffset == base.OffsetNull {
					deleteNodeFunc()
				} else {
					beforeNode, err := tree.OffsetLoadNode(node.BeforeNodeOffset)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.NoLeafDelete] tree.OffsetLoadNode 错误: %s", err.Error()))
						return 0, false, false, err
					}
					node.KeysOffsetList[0] = beforeNode.KeysOffsetList[len(beforeNode.KeysOffsetList)-1]
				}
			} else {
				hasLastChange = true
				if len(node.KeysValueList) > 1 {
					deleteKeyFunc(index)
				} else if node.Offset == base.RootOffsetValue {
					// root 节点需要特殊处理
					hasLastChange = true
					return 0, hasFirstChange, hasLastChange, nil
				} else if node.AfterNodeOffset == base.OffsetNull {
					deleteNodeFunc()
				} else {
					afterNode, err := tree.OffsetLoadNode(node.AfterNodeOffset)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.NoLeafDelete] tree.OffsetLoadNode 错误: %s", err.Error()))
						return 0, false, false, err
					}
					node.KeysOffsetList[len(node.KeysOffsetList)-1] = afterNode.KeysOffsetList[0]
				}
			}
		} else {
			deleteKeyFunc(index)
		}
	}
	return len(node.KeysOffsetList), hasFirstChange, hasLastChange, nil
}

func (tree *BPlusTree) NodeParentMap() (map[int64]*ParentInfo, base.StandardError) {
	r := make(map[int64]*ParentInfo, 0)
	queue := make([]*BPlusTreeNode, 0)
	queue = append(queue, tree.Root)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if node == nil {
			continue
		}
		if len(node.KeysOffsetList) > 0 {
			passLoad := false
			for i, offset := range node.KeysOffsetList {
				if offset != base.OffsetNull {
					if _, ok := r[offset]; !ok {
						r[offset] = &ParentInfo{
							LeftParent:  base.OffsetNull,
							OnlyParent:  base.OffsetNull,
							RightParent: base.OffsetNull,
						}
					}
					// 记录
					if i == 0 {
						r[offset].LeftParent = node.Offset
					} else if i == len(node.KeysOffsetList)-1 {
						r[offset].RightParent = node.Offset
					} else {
						r[offset].OnlyParent = node.Offset
					}

					if passLoad {
						// 叶子结点不用解析
						continue
					}
					childNode, err := tree.OffsetLoadNode(offset)
					if err != nil {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeParentMap.OffsetLoadNode]错误: %s", err.Error()))
						return nil, err
					}
					if childNode.IsLeaf == true {
						// 叶子结点不用解析
						passLoad = true
						continue
					}
					queue = append(queue, childNode)
				}
			}
		}
	}

	// 只有 left 或者只有 right 的需要归为 only
	for _, pInfo := range r {
		if pInfo.RightParent != base.OffsetNull && pInfo.LeftParent == base.OffsetNull {
			pInfo.OnlyParent = pInfo.RightParent
			pInfo.RightParent = base.OffsetNull
		} else if pInfo.LeftParent != base.OffsetNull && pInfo.RightParent == base.OffsetNull {
			pInfo.OnlyParent = pInfo.LeftParent
			pInfo.LeftParent = base.OffsetNull
		}
	}

	return r, nil
}

func (node *BPlusTreeNode) SprintBPlusTreeNode(tree *BPlusTree) (string, base.StandardError) {
	r := ""
	if !node.IsLeaf {
		if len(node.KeysOffsetList)-1 != len(node.KeysValueList) {
			errMsg := "非法非叶子结点，长度不对"
			utils.LogError("[SprintBPlusTreeNode] " + errMsg)
			return "", base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
		}
		r += fmt.Sprintf("[Index结点<%d>] : ", node.Offset)
		for i := 0; i < len(node.KeysValueList); i++ {
			offsetString := fmt.Sprint(node.KeysOffsetList[i])
			keyType := tree.TableInfo.PrimaryKeyFieldInfo.FieldType
			keyString := keyType.StringValue(node.KeysValueList[i].Value)
			r += fmt.Sprintf("offset: <%s> <== key<%s:%d>: <%s>; ", offsetString, tree.TableInfo.PrimaryKeyFieldInfo.Name, i, keyString)
		}
		lastOffsetString := fmt.Sprint(node.KeysOffsetList[len(node.KeysOffsetList)-1])
		r += fmt.Sprintf("lastOffset: <%s>; ", lastOffsetString)
	} else {
		if len(node.DataValues) != len(node.KeysValueList) {
			errMsg := "非法叶子结点，长度不对"
			utils.LogError("[SprintBPlusTreeNode] " + errMsg)
			return "", base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
		}
		valuesTypeMap, err := tree.TableInfo.ValueFieldInfoMap()
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[SprintBPlusTreeNode.tree.TableInfo.ValueFieldInfoMap]错误: %s", err.Error()))
			return "", err
		}
		r += fmt.Sprintf("[Leaf结点<%d>] : ", node.Offset)
		for i := 0; i < len(node.KeysValueList); i++ {
			r += fmt.Sprintf("item<%d>(", i)
			keyType := tree.TableInfo.PrimaryKeyFieldInfo.FieldType
			keyString := keyType.StringValue(node.KeysValueList[i].Value)
			r += fmt.Sprintf("pk<%s>: %s", tree.TableInfo.PrimaryKeyFieldInfo.Name, keyString)
			for name, v := range node.DataValues[i] {
				if valueTableInfo, ok := valuesTypeMap[name]; ok {
					valueString := valueTableInfo.FieldType.StringValue(v.Value)
					r += fmt.Sprintf("; value<%s>: <%s>", name, valueString)
				}

			}
			r += "); "
		}
	}
	utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[SprintBPlusTreeNode]完成: %s", r))
	return r, nil
}

// PrintBPlusTree 这个方法按照层级分行打印出B+树的每个节点的键值，方便查看B+树的结构。
func (tree *BPlusTree) PrintBPlusTree() base.StandardError {
	utils.LogWithoutInfo("PrintBPlusTree")
	utils.LogWithoutInfo("\n---**** PrintBPlusTree ****---\n")
	queue := make([]*BPlusTreeNode, 0) // 队列存放节点
	queue = append(queue, tree.Root)
	level := 0             // 当前节点所在的层数
	currentLevelCount := 1 // 当前层级节点数量
	nextLevelCount := 0    // 下一层级节点数量
	utils.LogWithoutInfo(fmt.Sprintf("-- Level %d: --\n", level))
	for len(queue) > 0 {
		node := queue[0]       // 取队列中的第一个节点
		queue = queue[1:]      // 从队列中删除第一个节点
		currentLevelCount -= 1 // 当前层级节点数量减1
		if node != nil {
			nodeLog, err := node.SprintBPlusTreeNode(tree)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[PrintBPlusTree.SprintBPlusTreeNode]错误: %s", err.Error()))
				return err
			}
			utils.LogWithoutInfo(fmt.Sprintf("    %s\n", nodeLog))
			if len(node.KeysOffsetList) > 0 {
				for _, offset := range node.KeysOffsetList {
					if offset != base.OffsetNull {
						nextLevelCount += 1
						childNode, err := tree.OffsetLoadNode(offset)
						if err != nil {
							utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[PrintBPlusTree.OffsetLoadNode]错误: %s", err.Error()))
							return err
						}
						queue = append(queue, childNode)
					}
				}
			}
		}
		if currentLevelCount == 0 { // 当前层级节点输出完毕时，进入下一层级
			level++
			currentLevelCount = nextLevelCount
			nextLevelCount = 0
			utils.LogWithoutInfo(fmt.Sprintf("-- Level %d: --\n", level))
		}
	}
	utils.LogWithoutInfo("\n---**** END ****---\n")
	utils.LogWithoutInfo("\n")
	return nil
}

// LoadBPlusTreeFromJson 用于加载整个B+树
func LoadBPlusTreeFromJson(jsonData []byte) (*BPlusTree, base.StandardError) {
	var (
		tree     = BPlusTree{}
		jsonTree = BPlusTreeJSON{}
	)

	if err := json.Unmarshal(jsonData, &jsonTree); err != nil {
		utils.LogError("[NodeToByteData] json.Unmarshal error: " + err.Error())
		return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, err)
	}

	// 1. 处理table info
	tableInfo, err := tableSchema.InitTableMetaInfoByJson(utils.ToJSON(jsonTree.RawTableInfo))
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[LoadBPlusTreeFromJson.tableSchema.InitTableMetaInfoByJson]错误: %s", err.Error()))
		return nil, err
	}
	tree.TableInfo = tableInfo

	// 2. 处理root node
	dataMap := make(map[int64][]byte, 0)
	rootNode := jsonTree.Root
	if rootNode == nil {
		errMsg := "rootNode 为空"
		utils.LogError("[NodeToByteData] %s", errMsg)
		return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
	}
	err = rootNode.GetValueAndKeyInfo(tableInfo)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[LoadBPlusTreeFromJson.rootNode.GetValueAndKeyInfo]错误: %s", err.Error()))
		return nil, err
	}
	tree.Root = rootNode.JSONTypeToOriginalType()
	byteData, err := tree.Root.NodeToByteData(tree.TableInfo)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[LoadBPlusTreeFromJson.node.GetValueAndKeyInfo] NodeToByteData错误: %s", err.Error()))
		return nil, err
	}
	dataMap[tree.Root.Offset] = byteData

	// 3. 处理非 root 的每个 node
	if jsonTree.ValueNode != nil && len(jsonTree.ValueNode) > 0 {
		for _, node := range jsonTree.ValueNode {
			if node == nil {
				errMsg := "node 为空"
				utils.LogError("[NodeToByteData] %s", errMsg)
				return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf(errMsg))
			}
			err = node.GetValueAndKeyInfo(tableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[LoadBPlusTreeFromJson.node.GetValueAndKeyInfo]错误: %s", err.Error()))
				return nil, err
			}
			originalNode := node.JSONTypeToOriginalType()
			byteData, err := originalNode.NodeToByteData(tree.TableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[LoadBPlusTreeFromJson.node.GetValueAndKeyInfo] NodeToByteData错误: %s", err.Error()))
				return nil, err
			}
			dataMap[originalNode.Offset] = byteData
		}
	}

	// 4. json加载的表, 添加使用的数据储存管理器
	dataManagerFunc, err := data_io.GetManagerInitFuncByType(tree.TableInfo.StorageType)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[LoadBPlusTreeFromJson] GetManagerInitFuncByType 错误: %s", err.Error()))
		return nil, err
	}
	manager, err := dataManagerFunc(dataMap, tableInfo.PageSize)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[LoadBPlusTreeFromJson] dataManagerFunc 错误: %s", err.Error()))
		return nil, err
	}
	tree.DataManager = manager

	// 5. 处理阶数
	tree.IndexOrder = jsonTree.IndexOrder
	tree.LeafOrder = jsonTree.LeafOrder

	return &tree, nil
}

func (node *BPlusTreeNode) BPlusTreeNodeToJson(tableInfo *tableSchema.TableMetaInfo) (string, base.StandardError) {
	b := BPlusTreeNodeJSON{
		BPlusTreeNode: *node,
	}
	err := b.GetValueAndKeyStringValue(tableInfo)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNodeToJson] GetValueAndKeyStringValue 错误: %s", err.Error()))
		return "", err
	}
	// 转化 json
	jsonByte, er := json.Marshal(b)
	if er != nil {
		utils.LogError(fmt.Sprintf("[BPlusTreeNodeToJson] json.Marshal 错误, %s", er.Error()))
		return "", base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, er)
	}
	return string(jsonByte), nil
}

func (tree *BPlusTree) BPlusTreeToJson() (string, base.StandardError) {
	var (
		jsonTree = BPlusTreeJSON{
			LeafOrder:  tree.LeafOrder,
			IndexOrder: tree.IndexOrder,
		}
		err base.StandardError
	)
	// 先处理table info
	err = tree.TableInfo.FillingRawFieldType()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeToJson] FillingRawFieldType 错误: %s", err.Error()))
	}
	jsonTree.RawTableInfo = tree.TableInfo

	// 处理 root
	rootJson := BPlusTreeNodeJSON{
		BPlusTreeNode: *tree.Root,
	}
	err = rootJson.GetValueAndKeyStringValue(tree.TableInfo)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeToJson] GetValueAndKeyStringValue 错误: %s", err.Error()))
		return "", err
	}
	jsonTree.Root = &rootJson

	// 处理每个 node
	jsonTree.ValueNode = make([]*BPlusTreeNodeJSON, 0)
	allNode, err := tree.LoadAllNode()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeToJson] tree.LoadAllNode 错误: %s", err.Error()))
		return "", err
	}
	for _, node := range allNode {
		if node != nil && node.Offset != base.RootOffsetValue {
			b := BPlusTreeNodeJSON{
				BPlusTreeNode: *node,
			}
			err = b.GetValueAndKeyStringValue(tree.TableInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeToJson] GetValueAndKeyStringValue 错误: %s", err.Error()))
				return "", err
			}
			jsonTree.ValueNode = append(jsonTree.ValueNode, &b)
		}
	}

	// 转化 json
	jsonByte, er := json.Marshal(jsonTree)
	if er != nil {
		utils.LogError(fmt.Sprintf("[BPlusTreeToJson] json.Marshal 错误, %s", er.Error()))
		return "", base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, er)
	}
	return string(jsonByte), nil
}

func (tree *BPlusTree) CompareBPlusTreesSame(tree2 *BPlusTree) (bool, base.StandardError) {
	var err base.StandardError

	if err = tree.TableInfo.Verification(); err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[CompareBPlusTrees] tree table info 非法"))
		return false, err
	}

	if err = tree2.TableInfo.Verification(); err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[CompareBPlusTrees] tree2 table info 非法"))
		return false, err
	}

	// 两树表不一致，则两个树不可能相同
	if !tree.TableInfo.CompareTableInfo(tree2.TableInfo) {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTrees] 两树表不一致"))
		return false, nil
	}

	// 如果阶数不同，则两个树不可能相同
	if tree.LeafOrder != tree2.LeafOrder || tree.IndexOrder != tree2.IndexOrder {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTrees] 两树阶数不同, tree.LeafOrder: %d, tree2.LeafOrder: %d, tree.IndexOrder: %d, tree2.IndexOrder: %d", tree.LeafOrder, tree2.LeafOrder, tree.IndexOrder, tree2.IndexOrder))
		return false, nil
	}

	// 根节点对比
	rootSame, err := tree.Root.CompareBPlusTreeNodesSame(tree2.Root)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[CompareBPlusTrees] CompareBPlusTreeNodesSame err: %s", err.Error()))
		return false, err
	}
	if !rootSame {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTrees] 两树根节点不一致")
		return false, nil
	}

	// 非根节点对比
	treeAllNode, err := tree.LoadAllNode()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[CompareBPlusTrees] LoadAllNode err: %s", err.Error()))
		return false, err
	}
	tree2AllNode, err := tree2.LoadAllNode()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[CompareBPlusTrees] LoadAllNode err: %s", err.Error()))
		return false, err
	}
	if len(treeAllNode) != len(tree2AllNode) {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTrees] 两树非根节点数量不一致, tree: %d, tree2: %d", len(treeAllNode), len(tree2AllNode)))
		return false, nil
	}
	for offset, node := range treeAllNode {
		if node2, ok := tree2AllNode[offset]; ok {
			nodeSame, err := node.CompareBPlusTreeNodesSame(node2)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[CompareBPlusTrees] CompareBPlusTreeNodesSame err: %s", err.Error()))
				return false, err
			}
			if !nodeSame {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTrees] 两树节点<%d>不一致", offset))
				return false, nil
			}
		} else {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTrees] offset <%d> 在tree2中不存在", offset))
			return false, nil
		}
	}

	// DataManager 是否一致不关心

	return true, nil
}

func (node *BPlusTreeNode) CompareBPlusTreeNodesSame(node2 *BPlusTreeNode) (bool, base.StandardError) {
	// IsLeaf
	if node.IsLeaf != node2.IsLeaf {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTreeNodes] 两节点 IsLeaf 不同")
		return false, nil
	}

	// Offset
	if node.Offset != node2.Offset {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTreeNodes] 两节点 Offset 不同")
		return false, nil
	}

	// BeforeNodeOffset
	if node.BeforeNodeOffset != node2.BeforeNodeOffset {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTreeNodes] 两节点 BeforeNodeOffset 不同")
		return false, nil
	}

	// AfterNodeOffset
	if node.AfterNodeOffset != node2.AfterNodeOffset {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTreeNodes] 两节点 AfterNodeOffset 不同")
		return false, nil
	}

	// KeysValueList
	if node.KeysValueList == nil && node2.KeysValueList == nil {
		// pass
	} else if (node.KeysValueList == nil || node2.KeysValueList == nil) || len(node.KeysValueList) != len(node2.KeysValueList) {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTreeNodes] 两节点 KeysValueList 不同 node: %#v, node2: %#v", node.KeysValueList, node2.KeysValueList))
		return false, nil
	}
	if node.KeysValueList != nil {
		for i, v := range node.KeysValueList {
			if v == nil {
				errMsg := fmt.Sprintf("node 的 KeysValueList 存在空值")
				utils.LogError(fmt.Sprintf("[CompareBPlusTreeNodesSame] %s", errMsg))
				return false, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerTypeError, fmt.Errorf(errMsg))
			}
			v2 := node2.KeysValueList[i]
			if v2 == nil {
				errMsg := fmt.Sprintf("node 的 KeysValueList 存在空值")
				utils.LogError(fmt.Sprintf("[CompareBPlusTreeNodesSame] %s", errMsg))
				return false, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerTypeError, fmt.Errorf(errMsg))
			}
			if !list.ByteListEqual(v.Value, v2.Value) {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTreeNodes] 两节点 KeysValueList 不同 node value: %#v, node2 value: %#v", v.Value, v2.Value))
				return false, nil
			}
		}
	}

	// KeysOffsetList
	if !node.IsLeaf {
		if node.KeysOffsetList == nil && node2.KeysOffsetList == nil {
			// pass
		} else if node.KeysOffsetList == nil || node2.KeysOffsetList == nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTreeNodesSame] 两节点 KeysOffsetList 不同")
			return false, nil
		} else if len(node.KeysOffsetList) != len(node2.KeysOffsetList) {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTreeNodesSame] 两节点 KeysOffsetList 不同")
			return false, nil
		} else if !list.Int64ListEqual(node.KeysOffsetList, node2.KeysOffsetList) {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTreeNodesSame] 两节点 KeysOffsetList 不同")
			return false, nil
		}
	}

	// DataValues
	if node.IsLeaf {
		if node.DataValues == nil && node2.DataValues == nil {
			// pass
		} else if node.DataValues == nil || node2.DataValues == nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTreeNodesSame] offset: %d 两节点 DataValues 不同, 其中一个DataValues为nil, node.DataValues is nil %v, node2.DataValues is nil %v", node.Offset, node.DataValues == nil, node2.DataValues == nil))
			return false, nil
		} else if len(node.DataValues) != len(node2.DataValues) {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)("[CompareBPlusTreeNodesSame] 两节点 DataValues 不同, 长度不一致")
			return false, nil
		} else {
			for i, v := range node.DataValues {
				v2 := node2.DataValues[i]
				if v == nil || v2 == nil || len(v) == 0 || len(v2) == 0 {
					errMsg := fmt.Sprintf("node 的 DataValues 存在非法空值")
					utils.LogError(fmt.Sprintf("[CompareBPlusTreeNodesSame] %s", errMsg))
					return false, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerTypeError, fmt.Errorf(errMsg))
				}
				for key, value := range v {
					if value2, ok := v2[key]; ok {
						if !list.ByteListEqual(value.Value, value2.Value) {
							utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTreeNodes] 两节点 DataValues 不同, value.Value: %+v, value2.Value: %+v", value.Value, value2.Value))
							return false, nil
						}
					} else {
						utils.LogDev(string(base.FunctionModelCoreBPlusTree), 5)(fmt.Sprintf("[CompareBPlusTreeNodesSame] 两节点 DataValues 不同, value key: %s 在 node2 不存在", key))
						return false, nil
					}
				}
			}
		}
	}

	utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[CompareBPlusTreeNodesSame] 两节点相同")
	return true, nil
}
