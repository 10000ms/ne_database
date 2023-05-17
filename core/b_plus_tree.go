package core

import (
	"encoding/json"
	"fmt"

	"ne_database/core/base"
	"ne_database/core/resource"
	tableSchema "ne_database/core/table_schema"
	"ne_database/utils"
)

type ValueInfo struct {
	Value []byte                `json:"value"` // 具体值
	Type  *tableSchema.MetaType `json:"type"`  // 值类型
}

// BPlusTree B+树结构体
type BPlusTree struct {
	Root            *BPlusTreeNode             // 根节点
	TableInfo       *tableSchema.TableMetaInfo // B+树对应的表信息
	LeafOrder       int                        // 叶子节点的B+树的阶数
	IndexOrder      int                        // 非叶子节点的B+树的阶数
	ResourceManager *resource.IOManager        // 资源文件的获取方法
}

type BPlusTreeNode struct {
	IsLeaf           bool                    `json:"is_leaf"`            // 是否为叶子节点
	KeysValueList    []*ValueInfo            `json:"keys_value_list"`    // key的index
	KeysOffsetList   []int64                 `json:"keys_offset_list"`   // index对应的子节点的offset列表，长度比KeysValueList +1，最后一个是尾部的offset
	DataValues       []map[string]*ValueInfo `json:"data_values"`        // 值列表: map[值名]值
	Offset           int64                   `json:"offset"`             // 该节点在硬盘文件中的偏移量，也是该节点的id
	BeforeNodeOffset int64                   `json:"before_node_offset"` // 该节点相连的前一个结点的偏移量
	AfterNodeOffset  int64                   `json:"after_node_offset"`  // 该节点相连的后一个结点的偏移量
	ParentOffset     int64                   `json:"parent_offset"`      // 该节点父结点偏移量
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

	lengthSuccess := len(data) >= startIndex+base.DataByteLengthOffset+primaryKeyInfo.Length
	if !lengthSuccess {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[getNoLeafNodeByteDataReadLoopData] 主键内容不够完成这轮解析，返回")
		return &r, nil
	} else {
		fieldValue := data[startIndex+base.DataByteLengthOffset : startIndex+base.DataByteLengthOffset+primaryKeyInfo.Length]
		fieldType := primaryKeyInfo.FieldType
		if fieldType.IsNull(fieldValue) {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[getNoLeafNodeByteDataReadLoopData] 主键为空，返回")
			return &r, nil
		} else {
			r.PrimaryKeySuccess = true
			r.PrimaryKey = &ValueInfo{
				Value: fieldValue,
				Type:  &primaryKeyInfo.FieldType,
			}
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[getNoLeafNodeByteDataReadLoopData] 全部解析完成，返回 ", utils.ToJSON(r))
			return &r, nil
		}
	}
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
	if !pkType.IsNull(pkValue) {
		errMsg := "主键数据为空"
		utils.LogError("[getLeafNodeByteDataReadLoopData] " + errMsg)
		return &r, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
	}
	r.PrimaryKeySuccess = true
	r.PrimaryKey = &ValueInfo{
		Value: pkValue,
		Type:  &primaryKeyInfo.FieldType,
	}
	// 3. 获取各个值的信息
	valueIndex += startIndex + primaryKeyInfo.Length
	r.Value = make(map[string]*ValueInfo, 0)
	for _, v := range valueInfo {
		r.Value[v.Name] = &ValueInfo{
			Value: data[startIndex+valueIndex : startIndex+valueIndex+v.Length],
			Type:  &v.FieldType,
		}
		valueIndex += v.Length
	}
	r.ValueSuccess = true
	utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)("[getLeafNodeByteDataReadLoopData] 全部解析完成，返回 ", utils.ToJSON(r))
	return &r, nil
}

func (tree *BPlusTree) LoadByteData(data map[int64][]byte) (map[int64]*BPlusTreeNode, base.StandardError) {
	if data == nil || len(data) == 0 {
		errMsg := "输入数据内容不对"
		utils.LogError("[BPlusTree LoadByteData] " + errMsg)
		return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
	}
	r := make(map[int64]*BPlusTreeNode, 0)
	for offset, pageData := range data {
		n := BPlusTreeNode{}
		err := n.LoadByteData(offset, tree.TableInfo, pageData)
		if err != nil {
			return nil, err
		}
		r[offset] = &n
	}
	return r, nil
}

func (tree *BPlusTree) OffsetLoadNode(offset int64) (*BPlusTreeNode, base.StandardError) {
	rm := *tree.ResourceManager
	nodeData, er := rm.Reader(offset)
	if er != nil {
		utils.LogError("[BPlusTreeNode OffsetToNode Reader] 读取数据错误 " + er.Error())
		return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, er)
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
		baseErr error
		err     base.StandardError
	)
	node.Offset = offset
	if len(data) != CoreConfig.PageSize {
		errMsg := "输入数据长度不对"
		utils.LogError("[BPlusTreeNode LoadByteData] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, fmt.Errorf(errMsg))
	}
	// 1. 加载第一位，判断是否是叶子结点
	if data[0] == base.NodeTypeIsLeaf {
		node.IsLeaf = true
	} else {
		node.IsLeaf = false
	}
	// 2. 加载这个节点的相邻两个节点的偏移量(offset)
	node.BeforeNodeOffset, baseErr = base.ByteListToInt64(data[1:5])
	if baseErr != nil {
		return err
	}
	node.AfterNodeOffset, baseErr = base.ByteListToInt64(data[len(data)-4:])
	// 3. 加载这个节点的实际数据
	data = data[5 : len(data)-4]
	// 循环次数
	loopTime := 0
	if !node.IsLeaf {
		// 运行数据
		loopData, err := getNoLeafNodeByteDataReadLoopData(data, loopTime, tableInfo.PrimaryKeyFieldInfo)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] getNoLeafNodeByteDataReadLoopData 出错, loopTime: <%d>", loopTime))
			return err
		}
		for true {
			if node.KeysOffsetList == nil {
				node.KeysOffsetList = make([]int64, 0)
			}
			if node.KeysValueList == nil {
				node.KeysValueList = make([]*ValueInfo, 0)
			}
			// 先检查是否符合退出条件
			if loopData.OffsetSuccess == false {
				break
			}
			node.KeysOffsetList = append(node.KeysOffsetList, loopData.Offset)
			if loopData.PrimaryKeySuccess == false || loopData.PrimaryKey == nil {
				break
			}
			node.KeysValueList = append(node.KeysValueList, loopData.PrimaryKey)
			loopData, err = getNoLeafNodeByteDataReadLoopData(data, loopTime, tableInfo.PrimaryKeyFieldInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] getNoLeafNodeByteDataReadLoopData 出错, loopTime: <%d>", loopTime))
				return err
			}
		}
	} else {
		// 运行数据
		loopData, err := getLeafNodeByteDataReadLoopData(data, loopTime, tableInfo.PrimaryKeyFieldInfo, tableInfo.ValueFieldInfo)
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] getLeafNodeByteDataReadLoopData 出错, loopTime: <%d>", loopTime))
			return err
		}
		for true {
			if node.KeysValueList == nil {
				node.KeysValueList = make([]*ValueInfo, 0)
			}
			if node.DataValues == nil {
				node.DataValues = make([]map[string]*ValueInfo, 0)
			}
			// 先检查是否符合退出条件
			if loopData.PrimaryKeySuccess == false || loopData.ValueSuccess == false {
				break
			}
			node.KeysValueList = append(node.KeysValueList, loopData.PrimaryKey)
			node.DataValues = append(node.DataValues, loopData.Value)
			loopData, err = getLeafNodeByteDataReadLoopData(data, loopTime, tableInfo.PrimaryKeyFieldInfo, tableInfo.ValueFieldInfo)
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[BPlusTreeNode.LoadByteData] getLeafNodeByteDataReadLoopData 出错, loopTime: <%d>", loopTime))
				return err
			}
		}
	}
	return nil
}

// NodeByteDataLength 判断一个结点转化成为byte数据的长度
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

func (node *BPlusTreeNode) NodeToByteData() ([]byte, base.StandardError) {
	var (
		d   = make([]byte, 0)
		err base.StandardError
	)
	// 1. 取is_leaf
	if node.IsLeaf {
		d = append(d, base.NodeTypeIsLeaf)
	} else {
		d = append(d, base.NodeTypeIsNotLeaf)
	}

	// 2. 取前一个结点的偏移量
	beforeNodeByte, err := base.Int64ToByteList(node.BeforeNodeOffset)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 取前一个结点的偏移量出错"))
		return nil, err
	}
	d = append(d, beforeNodeByte...)

	// 3. 取内容数据
	if !node.IsLeaf {
		if len(node.KeysOffsetList)-1 != len(node.KeysValueList) {
			errMsg := "非法非叶子结点，长度不对"
			utils.LogError("[NodeToByteData] " + errMsg)
			return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
		}
		for i := 0; i < len(node.KeysValueList); i++ {
			offsetByte, err := base.Int64ToByteList(node.KeysOffsetList[i])
			if err != nil {
				utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 取offsetByte出错"))
				return nil, err
			}
			d = append(d, offsetByte...)
			d = append(d, node.KeysValueList[i].Value...)
		}
		lastOffsetByte, err := base.Int64ToByteList(node.KeysOffsetList[len(node.KeysOffsetList)-1])
		if err != nil {
			utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 取lastOffsetByte出错"))
			return nil, err
		}
		d = append(d, lastOffsetByte...)
	} else {
		if len(node.DataValues) != len(node.KeysValueList) {
			errMsg := "非法叶子结点，长度不对"
			utils.LogError("[NodeToByteData] " + errMsg)
			return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
		}
		for i := 0; i < len(node.KeysValueList); i++ {
			d = append(d, node.KeysValueList[i].Value...)
			for _, v := range node.DataValues[i] {
				d = append(d, v.Value...)
			}
		}
	}

	// 4. 补齐中间空余部分
	if CoreConfig.PageSize < len(d)-base.DataByteLengthOffset {
		errMsg := "结点长度超长"
		utils.LogError("[NodeToByteData] " + errMsg)
		return nil, base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
	}
	d = append(d, make([]uint8, CoreConfig.PageSize-len(d)-base.DataByteLengthOffset)...)

	// 5. 取后一个结点的偏移量
	afterNodeByte, err := base.Int64ToByteList(node.AfterNodeOffset)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreBPlusTree), 10)(fmt.Sprintf("[NodeToByteData] 取后一个结点的偏移量出错"))
		return nil, err
	}
	d = append(d, afterNodeByte...)
	return d, nil
}

// Insert 插入键值对
func (tree *BPlusTree) Insert(key int64, value interface{}) {
	// 1. 查找插入位置
	curNode := tree.Root
	for !curNode.IsLeaf {
		index := 0
		for ; index < len(curNode.Keys); index++ {
			if curNode.Keys[index] > key {
				break
			}
		}
		curNode = curNode.Child[index]
	}

	// 2. 向叶子节点插入键值对
	index := 0
	for ; index < len(curNode.Keys); index++ {
		if curNode.Keys[index] >= key {
			break
		}
	}
	if index < len(curNode.Keys) && curNode.Keys[index] == key {
		curNode.Values[index] = value
	} else {
		curNode.Keys = append(curNode.Keys, 0)
		curNode.Values = append(curNode.Values, nil)
		copy(curNode.Keys[index+1:], curNode.Keys[index:])
		copy(curNode.Values[index+1:], curNode.Values[index:])
		curNode.Keys[index] = key
		curNode.Values[index] = value
	}

	// 3. 如果该叶子节点满了，进行分裂操作
	for len(curNode.Keys) == tree.Order {
		// 3.1. 分裂叶子节点
		parent := curNode.Parent
		newNode := &BPlusTreeNode{
			IsLeaf: true,
			Keys:   make([]int64, 0, tree.Order),
			Values: make([]interface{}, 0, tree.Order),
		}
		splitIndex := tree.Order / 2
		newNode.Keys = append(newNode.Keys, curNode.Keys[splitIndex:]...)
		newNode.Values = append(newNode.Values, curNode.Values[splitIndex:]...)
		curNode.Keys = curNode.Keys[:splitIndex]
		curNode.Values = curNode.Values[:splitIndex]

		// 3.2. 更新父节点的键列表和子节点列表
		if parent == nil {
			// 创建新的根节点
			newRoot := &BPlusTreeNode{
				IsLeaf: false,
				Keys:   []int64{newNode.Keys[0]},
				Child:  []*BPlusTreeNode{curNode, newNode},
				Parent: nil,
			}
			curNode.Parent = newRoot
			newNode.Parent = newRoot
			tree.Root = newRoot
		} else {
			// 更新父节点的键列表和子节点列表
			newNode.Parent = parent
			newKey := newNode.Keys[0]
			index := 0
			for ; index < len(parent.Keys); index++ {
				if parent.Keys[index] > newKey {
					break
				}
			}
			parent.Keys = append(parent.Keys, 0)
			parent.Child = append(parent.Child, nil)
			copy(parent.Keys[index+1:], parent.Keys[index:])
			copy(parent.Child[index+1:], parent.Child[index:])
			parent.Keys[index] = newKey
			parent.Child[index+1] = newNode
			if len(parent.Keys) == tree.Order {
				curNode = parent
			} else {
				break
			}
		}
	}
}

// Delete 删除键值对
func (tree *BPlusTree) Delete(key int64) {
	// 1. 查找对应的叶子节点
	curNode := tree.Root
	for !curNode.IsLeaf {
		index := 0
		for ; index < len(curNode.Keys); index++ {
			if curNode.Keys[index] > key {
				break
			}
		}
		curNode = curNode.Child[index]
	}

	// 2. 删除键值对
	index := 0
	for ; index < len(curNode.Keys); index++ {
		if curNode.Keys[index] == key {
			break
		}
	}
	if index < len(curNode.Keys) {
		copy(curNode.Keys[index:], curNode.Keys[index+1:])
		copy(curNode.Values[index:], curNode.Values[index+1:])
		curNode.Keys = curNode.Keys[:len(curNode.Keys)-1]
		curNode.Values = curNode.Values[:len(curNode.Values)-1]
	}

	// 3. 如果该叶子节点数量小于阶数的一半，需要合并或者转移
	for len(curNode.Keys) < tree.Order/2 && curNode != tree.Root {
		// 3.1. 找到兄弟节点
		parent := curNode.Parent
		index := 0
		for ; index < len(parent.Child); index++ {
			if parent.Child[index] == curNode {
				break
			}
		}
		var leftSibling, rightSibling *BPlusTreeNode
		if index > 0 {
			leftSibling = parent.Child[index-1]
		}
		if index < len(parent.Child)-1 {
			rightSibling = parent.Child[index+1]
		}

		// 3.2. 尝试向左兄弟节点转移
		if leftSibling != nil && len(leftSibling.Keys) > tree.Order/2 {
			curNode.Keys = append([]int64{0}, curNode.Keys...)
			curNode.Values = append([]interface{}{nil}, curNode.Values...)
			copy(curNode.Keys[0:], leftSibling.Keys[len(leftSibling.Keys)-1:])
			copy(curNode.Values[0:], leftSibling.Values[len(leftSibling.Values)-1:])
			leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
			leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]
			if !curNode.IsLeaf {
				curNode.Child = append([]*BPlusTreeNode{nil}, curNode.Child...)
				copy(curNode.Child[0:], leftSibling.Child[len(leftSibling.Child)-1:])
				leftSibling.Child[len(leftSibling.Child)-1].Parent = curNode
				leftSibling.Child = leftSibling.Child[:len(leftSibling.Child)-1]
			}
			parent.Keys[index-1] = curNode.Keys[0]
			break
		}

		// 3.3. 尝试向右兄弟节点转移
		if rightSibling != nil && len(rightSibling.Keys) > tree.Order/2 {
			curNode.Keys = append(curNode.Keys, 0)
			curNode.Values = append(curNode.Values, nil)
			copy(curNode.Keys[len(curNode.Keys)-1:], rightSibling.Keys[:1])
			copy(curNode.Values[len(curNode.Values)-1:], rightSibling.Values[:1])
			rightSibling.Keys = rightSibling.Keys[1:]
			rightSibling.Values = rightSibling.Values[1:]
			if !curNode.IsLeaf {
				curNode.Child = append(curNode.Child, nil)
				copy(curNode.Child[len(curNode.Child)-1:], rightSibling.Child[:1])
				rightSibling.Child[0].Parent = curNode
				rightSibling.Child = rightSibling.Child[1:]
			}
			parent.Keys[index] = rightSibling.Keys[0]
			break
		}

		// 3.4. 向左兄弟节点合并
		if leftSibling != nil {
			leftSibling.Keys = append(leftSibling.Keys, parent.Keys[index-1])
			leftSibling.Values = append(leftSibling.Values, nil)
			leftSibling.Keys = append(leftSibling.Keys, curNode.Keys...)
			leftSibling.Values = append(leftSibling.Values, curNode.Values...)
			if !curNode.IsLeaf {
				leftSibling.Child = append(leftSibling.Child, curNode.Child...)
				for _, child := range curNode.Child {
					child.Parent = leftSibling
				}
			}
			parent.Keys = append(parent.Keys[:index-1], parent.Keys[index:]...)
			parent.Child = append(parent.Child[:index], parent.Child[index+1:]...)
			curNode = parent
		} else { // 3.5. 向右兄弟节点合并
			curNode.Keys = append(curNode.Keys, parent.Keys[index])
			curNode.Values = append(curNode.Values, nil)
			curNode.Keys = append(curNode.Keys, rightSibling.Keys...)
			curNode.Values = append(curNode.Values, rightSibling.Values...)
			if !curNode.IsLeaf {
				curNode.Child = append(curNode.Child, rightSibling.Child...)
				for _, child := range rightSibling.Child {
					child.Parent = curNode
				}
			}
			parent.Keys = append(parent.Keys[:index], parent.Keys[index+1:]...)
			parent.Child = append(parent.Child[:index+1], parent.Child[index+2:]...)
			curNode = parent
		}
	}
	if len(tree.Root.Keys) == 0 {
		tree.Root = tree.Root.Child[0]
		tree.Root.Parent = nil
	}
}

// Search 查找键对应的值
func (tree *BPlusTree) Search(key int64) interface{} {
	curNode := tree.Root
	for curNode != nil {
		index := 0
		for ; index < len(curNode.Keys); index++ {
			if curNode.Keys[index] > key {
				break
			}
			if curNode.Keys[index] == key {
				return curNode.Values[index]
			}
		}
		if curNode.IsLeaf {
			break
		}
		curNode = curNode.Child[index]
	}
	return nil
}

func (node *BPlusTreeNode) SprintBPlusTreeNode(tree *BPlusTree) (string, base.StandardError) {
	r := ""
	if !node.IsLeaf {
		if len(node.KeysOffsetList)-1 != len(node.KeysValueList) {
			errMsg := "非法非叶子结点，长度不对"
			utils.LogError("[SprintBPlusTreeNode] " + errMsg)
			return "", base.NewDBError(base.FunctionModelCoreBPlusTree, base.ErrorTypeSystem, base.ErrorBaseCodeInnerDataError, fmt.Errorf(errMsg))
		}
		r += fmt.Sprintf("[Leaf结点<%d>] : ", node.Offset)
		for i := 0; i < len(node.KeysValueList); i++ {
			offsetString := fmt.Sprint(node.KeysOffsetList[i])
			keyType := *node.KeysValueList[i].Type
			keyString := keyType.LogString(node.KeysValueList[i].Value)
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
		r += fmt.Sprintf("[index结点<%d>] : ", node.Offset)
		for i := 0; i < len(node.KeysValueList); i++ {
			r += fmt.Sprintf("item<%d>(", i)
			keyType := *node.KeysValueList[i].Type
			keyString := keyType.LogString(node.KeysValueList[i].Value)
			r += fmt.Sprintf("pk<%s>: %s", tree.TableInfo.PrimaryKeyFieldInfo.Name, keyString)
			for name, v := range node.DataValues[i] {
				valueType := *v.Type
				valueString := valueType.LogString(v.Value)
				r += fmt.Sprintf("; value<%s>: <%s>", name, valueString)
			}
			r += "); "
		}
	}
	utils.LogDev(string(base.FunctionModelCoreBPlusTree), 1)(fmt.Sprintf("[SprintBPlusTreeNode]完成: %s", r))
	return r, nil
}

// PrintBPlusTree 这个方法按照层级分行打印出B+树的每个节点的键值，方便查看B+树的结构。
func (tree *BPlusTree) PrintBPlusTree() base.StandardError {
	utils.LogInfo("PrintBPlusTree")
	utils.LogInfo("\n---**** PrintBPlusTree ****---\n")
	queue := make([]*BPlusTreeNode, 0) // 队列存放节点
	queue = append(queue, tree.Root)
	level := 0             // 当前节点所在的层数
	currentLevelCount := 1 // 当前层级节点数量
	nextLevelCount := 0    // 下一层级节点数量
	utils.LogInfo("-- Level %d: --\n", level)
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
			utils.LogInfo(fmt.Sprintf("    %s\n", nodeLog))
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
			fmt.Printf("-- Level %d: --\n", level)
		}
	}
	fmt.Printf("\n---**** END ****---\n")
	fmt.Printf("\n")
	return nil
}

// LoadBPlusTreeFromJson 用于加载整个B+树
func LoadBPlusTreeFromJson(jsonData []byte) (*BPlusTree, error) {
	root, err := JsonToBPlusTree(jsonData)
	if err != nil {
		return nil, err
	}
	tree := &BPlusTree{
		Root:       root,
		LeafOrder:  0, // 使用默认的阶数  FIXME：这里不能使用 0 作为阶数，进行功能验证会出现问题，需要加载真实的阶数
		IndexOrder: 0, // 使用默认的阶数  FIXME：这里不能使用 0 作为阶数，进行功能验证会出现问题，需要加载真实的阶数
	}
	return tree, nil
}

// JsonToBPlusTree 用于将JSON数据转换为B+树的节点
func JsonToBPlusTree(jsonData []byte) (*BPlusTreeNode, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, err
	}
	node := &BPlusTreeNode{
		IsLeaf: false,
		Keys:   make([]int64, 0),
		Values: make([]interface{}, 0),
		Child:  make([]*BPlusTreeNode, 0),
		Parent: nil,
	}
	if _, ok := data[JSONKeyIsLeaf]; ok {
		node.IsLeaf = data[JSONKeyIsLeaf].(bool)
	} else {
		return nil, fmt.Errorf("[JsonToBPlusTree] 缺失键：%s", JSONKeyIsLeaf)
	}
	if _, ok := data[JSONKeyKeys]; ok {
		node.Keys = make([]int64, len(data[JSONKeyKeys].([]interface{})))
		for i, key := range data[JSONKeyKeys].([]interface{}) {
			node.Keys[i] = int64(key.(float64))
		}
	} else {
		return nil, fmt.Errorf("[JsonToBPlusTree] 缺失键：%s", JSONKeyKeys)
	}
	if _, ok := data[JSONKeyValues]; ok {
		node.Values = data[JSONKeyValues].([]interface{})
	} else {
		return nil, fmt.Errorf("[JsonToBPlusTree] 缺失键：%s", JSONKeyValues)
	}
	if childDataArray, ok := data[JSONKeyChild].([]interface{}); ok {
		for _, childData := range childDataArray {
			if childValue, ok := childData.(map[string]interface{}); ok {
				child, err := JsonToBPlusTree([]byte(utils.ToJSON(childValue)))
				if err != nil {
					utils.LogError(err)
					return nil, err
				}
				child.Parent = node
				node.Child = append(node.Child, child)
			} else {
				return nil, fmt.Errorf("[JsonToBPlusTree] Invalid child data")
			}
		}
	}
	return node, nil
}

func (tree *BPlusTree) CompareBPlusTrees(tree2 *BPlusTree) bool {
	// 确保两个树都是空的
	if (tree.Root == nil || tree2.Root == nil) && tree.Root != tree2.Root {
		utils.LogDebug("[CompareBPlusTrees] 两树Root不同")
		return false
	}

	// 如果阶数不同，则两个树不可能相同
	if tree.Order != tree2.Order {
		utils.LogDebug("[CompareBPlusTrees] 两树阶数不同")
		return false
	}

	// 从对比两个树的根节点开始
	return tree.Root.CompareBPlusTreeNodes(tree2.Root)
}

func (node *BPlusTreeNode) CompareBPlusTreeNodes(node2 *BPlusTreeNode) bool {
	// 父节点不对比
	// 因为对比一般自上而下，再去对比父节点无意义
	// 单独对比的时候，再去对比父节点反而会影响判断

	// 对比是否叶子节点
	if node.IsLeaf != node2.IsLeaf {
		utils.LogDebug("[CompareBPlusTreeNodes] 两节点IsLeaf不同")
		return false
	}

	// 对比key
	if utils.ToJSON(node.Keys) != utils.ToJSON(node2.Keys) {
		utils.LogDebug("[CompareBPlusTreeNodes] 两节点Keys不同")
		return false
	}

	// 对比value
	if utils.ToJSON(node.Values) != utils.ToJSON(node2.Values) {
		utils.LogDebug("[CompareBPlusTreeNodes] 两节点Values不同")
		return false
	}

	// 对于每个叶子节点，比较它所属的两个子树是否相同
	for i, childNode := range node.Child {
		childNode2 := node2.Child[i]
		if !childNode.CompareBPlusTreeNodes(childNode2) {
			return false
		}
	}
	return true
}
