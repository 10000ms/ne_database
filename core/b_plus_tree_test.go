package core

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"ne_database/core/base"
	"ne_database/core/resource"
	tableSchema "ne_database/core/table_schema"
	"ne_database/utils"
)

func TestGetNoLeafNodeByteDataReadLoopData(t *testing.T) {
	// 初始化一下
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")

	// 构造测试数据
	data := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // KeysOffset: 50
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key: "a"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // KeysOffset: 100
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key: "b"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc8, // KeysOffset: 200
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key: null
	}
	loopTime := 0 // 测试第0次解析
	primaryKeyInfo := &tableSchema.FieldInfo{
		Name:      "id",
		Length:    4 * 2, // 假设最长2字
		FieldType: tableSchema.StringType,
	}

	// 调用被测试函数
	result, err := getNoLeafNodeByteDataReadLoopData(data, loopTime, primaryKeyInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result.Offset != 50 {
		t.Errorf("expected offset to be 1, but got %d", result.Offset)
	}
	if !result.OffsetSuccess {
		t.Errorf("expected offsetSuccess to be true, but got false")
	}
	if !result.PrimaryKeySuccess {
		t.Errorf("expected primaryKeySuccess to be true, but got false")
	}
	if string(result.PrimaryKey.Value) != "a" {
		t.Errorf("expected primaryKey value to be 'a', but got %q", result.PrimaryKey.Value)
	}

	// 测试第1次解析
	loopTime = 1
	// 调用被测试函数
	result, err = getNoLeafNodeByteDataReadLoopData(data, loopTime, primaryKeyInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result.Offset != 100 {
		t.Errorf("expected offset to be 1, but got %d", result.Offset)
	}
	if !result.OffsetSuccess {
		t.Errorf("expected offsetSuccess to be true, but got false")
	}
	if !result.PrimaryKeySuccess {
		t.Errorf("expected primaryKeySuccess to be true, but got false")
	}
	if string(result.PrimaryKey.Value) != "b" {
		t.Errorf("expected primaryKey value to be 'b', but got %q", result.PrimaryKey.Value)
	}

	// 测试第2次解析
	loopTime = 2
	// 调用被测试函数
	result, err = getNoLeafNodeByteDataReadLoopData(data, loopTime, primaryKeyInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result.Offset != 200 {
		t.Errorf("expected offset to be 1, but got %d", result.Offset)
	}
	if !result.OffsetSuccess {
		t.Errorf("expected offsetSuccess to be true, but got false")
	}
	if result.PrimaryKeySuccess {
		t.Errorf("expected primaryKeySuccess to be true, but got false")
	}

	// 测试第3次解析
	loopTime = 3
	// 调用被测试函数
	result, err = getNoLeafNodeByteDataReadLoopData(data, loopTime, primaryKeyInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result.OffsetSuccess {
		t.Errorf("expected offsetSuccess to be true, but got false")
	}
	if result.PrimaryKeySuccess {
		t.Errorf("expected primaryKeySuccess to be true, but got false")
	}

}

func TestGetLeafNodeByteDataReadLoopData(t *testing.T) {
	// 初始化一下
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")

	// 构造测试数据
	data := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // id: 1
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // id: 2
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	loopTime := 0 // 测试第0次解析
	tableInfo := &tableSchema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableSchema.Int64Type,
		},
		ValueFieldInfo: []*tableSchema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableSchema.StringType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableSchema.StringType,
			},
		},
	}

	result, err := getLeafNodeByteDataReadLoopData(data, loopTime, tableInfo.PrimaryKeyFieldInfo, tableInfo.ValueFieldInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !result.PrimaryKeySuccess {
		t.Errorf("expected PrimaryKeySuccess to be true, but got false")
		return
	}
	if !result.ValueSuccess {
		t.Errorf("expected ValueSuccess to be true, but got false")
		return
	}
	pk, err := base.ByteListToInt64(result.PrimaryKey.Value)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if pk != 1 {
		t.Errorf("expected PrimaryKey is 1")
		return
	}
	age, err := base.ByteListToString(result.Value["age"].Value)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if age != "20" {
		t.Errorf("expected age is '20'")
		return
	}
	name, err := base.ByteListToString(result.Value["name"].Value)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if name != "Alice" {
		t.Errorf("expected name is 'Alice'")
		return
	}

	// 测试第1次解析
	loopTime = 1
	result, err = getLeafNodeByteDataReadLoopData(data, loopTime, tableInfo.PrimaryKeyFieldInfo, tableInfo.ValueFieldInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !result.PrimaryKeySuccess {
		t.Errorf("expected PrimaryKeySuccess to be true, but got false")
		return
	}
	if !result.ValueSuccess {
		t.Errorf("expected ValueSuccess to be true, but got false")
		return
	}
	pk, err = base.ByteListToInt64(result.PrimaryKey.Value)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if pk != 2 {
		t.Errorf("expected PrimaryKey is 2")
		return
	}
	age, err = base.ByteListToString(result.Value["age"].Value)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if age != "22" {
		t.Errorf("expected age is '22'")
		return
	}
	name, err = base.ByteListToString(result.Value["name"].Value)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if name != "Bob" {
		t.Errorf("expected name is 'Bob'")
		return
	}

	// 测试第2次解析
	loopTime = 2
	result, err = getLeafNodeByteDataReadLoopData(data, loopTime, tableInfo.PrimaryKeyFieldInfo, tableInfo.ValueFieldInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result.PrimaryKeySuccess {
		t.Errorf("expected PrimaryKeySuccess to be true, but got false")
		return
	}
	if result.ValueSuccess {
		t.Errorf("expected ValueSuccess to be true, but got false")
		return
	}
}

func TestBPlusTreeNode_LoadByteData(t *testing.T) {
	// 初始化一下
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	// 构造测试数据1
	data := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // BeforeNodeOffset: 50
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // id: 1
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // id: 2
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	data = append(data, make([]uint8, pageSize-len(data)-base.DataByteLengthOffset)...)
	data = append(data, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x96, // AfterNodeOffset: 150
	}...)

	tableInfo1 := &tableSchema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableSchema.Int64Type,
		},
		ValueFieldInfo: []*tableSchema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableSchema.StringType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableSchema.StringType,
			},
		},
	}

	key1, err := base.Int64ToByteList(int64(1))
	if err != nil {
		t.Errorf("Int64ToByteList Error: %v", err)
		return
	}

	key2, err := base.Int64ToByteList(int64(2))
	if err != nil {
		t.Errorf("Int64ToByteList Error: %v", err)
		return
	}

	offset := int64(100)
	parentOffset := int64(200)
	node := &BPlusTreeNode{
		IsLeaf:         true,
		KeysValueList:  []*ValueInfo{{Value: key1}, {Value: key2}},
		KeysOffsetList: nil,
		DataValues: []map[string]*ValueInfo{
			{
				"name": {Value: []byte("Alice")},
				"age":  {Value: []byte("20")},
			},
			{
				"name": {Value: []byte("Bob")},
				"age":  {Value: []byte("22")},
			},
		},
		Offset:           offset,
		BeforeNodeOffset: 50,
		AfterNodeOffset:  150,
		ParentOffset:     parentOffset,
	}

	targetNode := &BPlusTreeNode{}
	err = targetNode.LoadByteData(offset, parentOffset, tableInfo1, data)
	jsonNode, err := targetNode.BPlusTreeNodeToJson(tableInfo1)
	if err != nil {
		t.Errorf("BPlusTreeNodeToJson Error: %v", err)
		return
	}
	utils.LogDebug(fmt.Sprintf("targetNode: %s", jsonNode))
	if err != nil {
		t.Errorf("LoadByteData Error: %v", err)
		return
	}
	isEqual, err := node.CompareBPlusTreeNodesSame(targetNode)
	if err != nil {
		t.Error("CompareBPlusTreeNodesSame Expected nil error, but got error")
		return
	}
	if !isEqual {
		t.Error("CompareBPlusTreeNodesSame Expected true, but got false ")
		return
	}

	// 构造测试数据2
	data = []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xf4, // BeforeNodeOffset: 500
		0x00,                                           // IsLeaf: false
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // node value length: 3
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // KeysOffset: 50
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key: "a"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // KeysOffset: 100
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key: "b"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc8, // KeysOffset: 200
	}
	data = append(data, make([]uint8, pageSize-len(data)-base.DataByteLengthOffset)...)
	data = append(data, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0xdc, // AfterNodeOffset: 1500
	}...)

	tableInfo2 := &tableSchema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
			Name:      "id",
			Length:    4 * 2, // 假设最长2字
			FieldType: tableSchema.StringType,
		},
		ValueFieldInfo: []*tableSchema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableSchema.StringType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableSchema.StringType,
			},
		},
	}

	keyA, err := base.StringToByteList("a")
	if err != nil {
		t.Errorf("StringToByteList Error: %v", err)
		return
	}

	keyB, err := base.StringToByteList("b")
	if err != nil {
		t.Errorf("StringToByteList Error: %v", err)
		return
	}

	offset = int64(1000)
	parentOffset = int64(2000)
	node = &BPlusTreeNode{
		IsLeaf:           false,
		KeysValueList:    []*ValueInfo{{Value: keyA}, {Value: keyB}},
		KeysOffsetList:   []int64{50, 100, 200},
		DataValues:       nil,
		Offset:           offset,
		BeforeNodeOffset: 500,
		AfterNodeOffset:  1500,
		ParentOffset:     parentOffset,
	}

	targetNode = &BPlusTreeNode{}
	err = targetNode.LoadByteData(offset, parentOffset, tableInfo2, data)
	jsonNode, err = targetNode.BPlusTreeNodeToJson(tableInfo2)
	if err != nil {
		t.Errorf("BPlusTreeNodeToJson Error: %v", err)
		return
	}
	utils.LogDebug(fmt.Sprintf("targetNode: %s", jsonNode))
	if err != nil {
		t.Errorf("LoadByteData Error: %v", err)
		return
	}
	isEqual, err = node.CompareBPlusTreeNodesSame(targetNode)
	if err != nil {
		t.Error("CompareBPlusTreeNodesSame Expected nil error, but got error")
		return
	}
	if !isEqual {
		t.Error("CompareBPlusTreeNodesSame Expected true, but got false ")
		return
	}

}

func TestBPlusTreeNode_NodeToByteData(t *testing.T) {
	// 初始化一下
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	tableInfo1 := &tableSchema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableSchema.Int64Type,
		},
		ValueFieldInfo: []*tableSchema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableSchema.StringType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableSchema.StringType,
			},
		},
	}

	key1, err := base.Int64ToByteList(int64(1))
	if err != nil {
		t.Errorf("Int64ToByteList Error: %v", err)
		return
	}

	key2, err := base.Int64ToByteList(int64(2))
	if err != nil {
		t.Errorf("Int64ToByteList Error: %v", err)
		return
	}

	node := &BPlusTreeNode{
		IsLeaf:         true,
		KeysValueList:  []*ValueInfo{{Value: key1}, {Value: key2}},
		KeysOffsetList: []int64{0, 8},
		DataValues: []map[string]*ValueInfo{
			{
				"name": {Value: []byte("Alice")},
				"age":  {Value: []byte("20")},
			},
			{
				"name": {Value: []byte("Bob")},
				"age":  {Value: []byte("22")},
			},
		},
		Offset:           100,
		BeforeNodeOffset: 50,
		AfterNodeOffset:  150,
		ParentOffset:     200,
	}

	data, err := node.NodeToByteData(tableInfo1)
	if err != nil {
		t.Errorf("Error: %v", err)
		return
	}
	expected := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // BeforeNodeOffset: 50
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // id: 1
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // id: 2
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"

	}
	expected = append(expected, make([]uint8, pageSize-len(expected)-base.DataByteLengthOffset)...)
	expected = append(expected, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x96, // AfterNodeOffset: 150
	}...)
	utils.LogDebug(fmt.Sprintf("expected []byte  %v", expected))
	if !bytes.Equal(data, expected) {
		t.Errorf("Expected: %v \n\t\t\t\t\t  but got: %v", expected, data)
		return
	}

	tableInfo2 := &tableSchema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
			Name:      "id",
			Length:    4 * 2, // 假设最长2字
			FieldType: tableSchema.StringType,
		},
		ValueFieldInfo: []*tableSchema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableSchema.StringType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableSchema.StringType,
			},
		},
	}

	keyA, err := base.StringToByteList("a")
	if err != nil {
		t.Errorf("StringToByteList Error: %v", err)
		return
	}

	keyB, err := base.StringToByteList("b")
	if err != nil {
		t.Errorf("StringToByteList Error: %v", err)
		return
	}

	node = &BPlusTreeNode{
		IsLeaf:           false,
		KeysValueList:    []*ValueInfo{{Value: keyA}, {Value: keyB}},
		KeysOffsetList:   []int64{50, 100, 200},
		DataValues:       nil,
		Offset:           1000,
		BeforeNodeOffset: 500,
		AfterNodeOffset:  1500,
		ParentOffset:     2000,
	}
	data, err = node.NodeToByteData(tableInfo2)
	if err != nil {
		t.Errorf("Error: %v", err)
		return
	}
	expected = []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xf4, // BeforeNodeOffset: 500
		0x00,                                           // IsLeaf: false
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // node length: 3
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // KeysOffset: 50
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key: "a"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // KeysOffset: 100
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key: "b"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc8, // KeysOffset: 200
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key: null
	}
	expected = append(expected, make([]uint8, pageSize-len(expected)-base.DataByteLengthOffset)...)
	expected = append(expected, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0xdc, // AfterNodeOffset: 1500
	}...)
	utils.LogDebug(fmt.Sprintf("expected []byte  %v", expected))
	if !bytes.Equal(data, expected) {
		t.Errorf("Expected: %v \n\t\t\t\t\t  but got: %v", expected, data)
		return
	}
}

func TestCompareBPlusTreesSame(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")

	resourceMap := make(map[int64][]byte, 0)

	// 创建两个相同的B+树
	tree1 := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("hello")},
				{Value: []byte("world")},
			},
			KeysOffsetList: []int64{},
			DataValues: []map[string]*ValueInfo{
				{
					"name": &ValueInfo{Value: []byte("Alice")},
					"age":  &ValueInfo{Value: []byte("18")},
				},
				{
					"name": &ValueInfo{Value: []byte("Bob")},
					"age":  &ValueInfo{Value: []byte("22")},
				},
			},
			Offset:           123,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    8,
				FieldType: tableSchema.Int64Type,
			},
			ValueFieldInfo: []*tableSchema.FieldInfo{
				{
					Name:      "name",
					Length:    4 * 20, // 假设最长20字节
					FieldType: tableSchema.StringType,
				},
				{
					Name:      "age",
					Length:    4 * 5, // 假设最长20字节
					FieldType: tableSchema.StringType,
				},
			},
		},
		LeafOrder:       1,
		IndexOrder:      1,
		ResourceManager: resource.InitMemoryConfig(resourceMap),
	}
	tree2 := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("hello")},
				{Value: []byte("world")},
			},
			KeysOffsetList: []int64{},
			DataValues: []map[string]*ValueInfo{
				{
					"name": &ValueInfo{Value: []byte("Alice")},
					"age":  &ValueInfo{Value: []byte("18")},
				},
				{
					"name": &ValueInfo{Value: []byte("Bob")},
					"age":  &ValueInfo{Value: []byte("22")},
				},
			},
			Offset:           123,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    8,
				FieldType: tableSchema.Int64Type,
			},
			ValueFieldInfo: []*tableSchema.FieldInfo{
				{
					Name:      "name",
					Length:    4 * 20, // 假设最长20字节
					FieldType: tableSchema.StringType,
				},
				{
					Name:      "age",
					Length:    4 * 5, // 假设最长20字节
					FieldType: tableSchema.StringType,
				},
			},
		},
		LeafOrder:       1,
		IndexOrder:      1,
		ResourceManager: resource.InitMemoryConfig(resourceMap),
	}

	// 对比两个树，期望为true
	isSame, err := tree1.CompareBPlusTreesSame(&tree2)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isSame {
		t.Error("Expected false, but got true ")
	}

	// TODO 这里需要加入 ResourceManager 里面有内容的对比
	//// 修改一个叶子节点的值，使得两棵树不同
	//node := tree1.Root.FindLeafNode(10)
	//node.Entries[0].Value = []byte("world")
	//err = tree1.UpdateNode(node)
	//assert.NoError(t, err)
	//
	//// 对比两个树，期望为false
	//isSame, err = tree1.CompareBPlusTreesSame(tree2)
	//assert.NoError(t, err)
	//assert.False(t, isSame)
}

func TestCompareBPlusTreeNodesSame(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")

	node1 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
		ParentOffset:     0,
	}

	node2 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
		ParentOffset:     0,
	}

	isEqual, err := node1.CompareBPlusTreeNodesSame(&node2)
	if !isEqual || err != nil {
		t.Errorf("Expected true and nil error, but got %v and %v", isEqual, err)
	}

	node3 := BPlusTreeNode{
		IsLeaf: false,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
		ParentOffset:     0,
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node3)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}

	node4 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello1")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
		ParentOffset:     0,
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node4)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}

	node5 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 301},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
		ParentOffset:     0,
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node5)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}

	node6 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("19")},
			},
			{
				"name": &ValueInfo{Value: []byte("John")},
				"age":  &ValueInfo{Value: []byte("25")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
		ParentOffset:     0,
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node6)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}

	node7 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           124,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
		ParentOffset:     0,
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node7)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}

	node8 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 457,
		AfterNodeOffset:  789,
		ParentOffset:     0,
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node8)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}

	node9 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  788,
		ParentOffset:     0,
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node9)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}

	node10 := BPlusTreeNode{
		IsLeaf: true,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList: []int64{100, 200, 300},
		DataValues: []map[string]*ValueInfo{
			{
				"name": &ValueInfo{Value: []byte("Alice")},
				"age":  &ValueInfo{Value: []byte("18")},
			},
			{
				"name": &ValueInfo{Value: []byte("Bob")},
				"age":  &ValueInfo{Value: []byte("22")},
			},
		},
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  787,
		ParentOffset:     1,
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node10)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}
}
