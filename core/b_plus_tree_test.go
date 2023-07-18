package core

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"ne_database/core/base"
	"ne_database/core/config"
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
	if !result.PrimaryKeySuccess {
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
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

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
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

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

func TestBPlusTreeNode_BPlusTreeNodeToJson(t *testing.T) {
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

	jsonNode, err := node.BPlusTreeNodeToJson(tableInfo1)
	if err != nil {
		t.Errorf("BPlusTreeNodeToJson Error: %v", err)
		return
	}
	utils.LogDebug(fmt.Sprintf("targetNode: %s", jsonNode))
	expJson := "{\"is_leaf\":true,\"keys_offset_list\":[0,8],\"offset\":100,\"before_node_offset\":50,\"after_node_offset\":150,\"parent_offset\":200,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"Bob\"}]}"
	if jsonNode != expJson {
		t.Errorf("BPlusTreeNodeToJson no same expect: %s, get: %s", expJson, jsonNode)
		return
	}

}

func TestLoadBPlusTreeFromJson(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	resourceMap := make(map[int64][]byte, 0)

	m1 := []byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // BeforeNodeOffset: -1
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m1 = append(m1, make([]uint8, pageSize-len(m1)-base.DataByteLengthOffset)...)
	m1 = append(m1, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0, // AfterNodeOffset: 2000
	}...)

	m2 := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, // BeforeNodeOffset: 1000
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m2 = append(m2, make([]uint8, pageSize-len(m2)-base.DataByteLengthOffset)...)
	m2 = append(m2, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0xb8, // AfterNodeOffset: 3000
	}...)

	m3 := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0, // BeforeNodeOffset: 2000
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m3 = append(m3, make([]uint8, pageSize-len(m3)-base.DataByteLengthOffset)...)
	m3 = append(m3, []byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // AfterNodeOffset: -1
	}...)

	// resourceMap放进去这些初始值
	resourceMap[1000] = m1
	resourceMap[2000] = m2
	resourceMap[3000] = m3

	// 创建B+树
	tree := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("aa")},
				{Value: []byte("bb")},
			},
			KeysOffsetList:   []int64{1000, 2000, 3000},
			DataValues:       nil,
			Offset:           0,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    4 * 2,
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
		},
		LeafOrder:       1,
		IndexOrder:      1,
		ResourceManager: resource.InitMemoryConfig(resourceMap),
	}

	jsonString := "{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[1000,2000,3000],\"offset\":0,\"before_" +
		"node_offset\":456,\"after_node_offset\":789,\"parent_offset\":0,\"keys_value\":[\"aa\",\"bb\"],\"data_value" +
		"s\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":" +
		"-1,\"after_node_offset\":2000,\"parent_offset\":0,\"keys_value\":[\"a\",\"b\"],\"data_values\":[{\"age\":\"20" +
		"\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"Bob\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offs" +
		"et\":2000,\"before_node_offset\":1000,\"after_node_offset\":3000,\"parent_offset\":0,\"keys_value\":[\"a\",\"b" +
		"\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"Bob\"}]},{\"is_leaf\":tru" +
		"e,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"parent_offs" +
		"et\":0,\"keys_value\":[\"a\",\"b\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"" +
		"name\":\"Bob\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"defau" +
		"lt\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"strin" +
		"g\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":1,\"index_order\":1}"

	tree2, err := LoadBPlusTreeFromJson([]byte(jsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// 对比两个树，期望为true
	isSame, err := tree.CompareBPlusTreesSame(tree2)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if !isSame {
		t.Error("Expected true, but got false ")
	}
}

func TestBPlusTree_BPlusTreeToJson(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	resourceMap := make(map[int64][]byte, 0)

	m1 := []byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // BeforeNodeOffset: -1
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m1 = append(m1, make([]uint8, pageSize-len(m1)-base.DataByteLengthOffset)...)
	m1 = append(m1, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0, // AfterNodeOffset: 2000
	}...)

	m2 := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, // BeforeNodeOffset: 1000
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m2 = append(m2, make([]uint8, pageSize-len(m2)-base.DataByteLengthOffset)...)
	m2 = append(m2, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0xb8, // AfterNodeOffset: 3000
	}...)

	m3 := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0, // BeforeNodeOffset: 2000
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m3 = append(m3, make([]uint8, pageSize-len(m3)-base.DataByteLengthOffset)...)
	m3 = append(m3, []byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // AfterNodeOffset: -1
	}...)

	// resourceMap放进去这些初始值
	resourceMap[1000] = m1
	resourceMap[2000] = m2
	resourceMap[3000] = m3

	// 创建B+树
	tree := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("aa")},
				{Value: []byte("bb")},
			},
			KeysOffsetList:   []int64{1000, 2000, 3000},
			DataValues:       nil,
			Offset:           0,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    4 * 2,
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
		},
		LeafOrder:       1,
		IndexOrder:      1,
		ResourceManager: resource.InitMemoryConfig(resourceMap),
	}

	treeJson, err := tree.BPlusTreeToJson()
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	err = tree.PrintBPlusTree()
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	utils.LogDebug(fmt.Sprintf("treeJson: %s", treeJson))

	tree2, err := LoadBPlusTreeFromJson([]byte(treeJson))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// 对比两个树，期望为true
	isSame, err := tree.CompareBPlusTreesSame(tree2)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if !isSame {
		t.Error("Expected true, but got false ")
	}
}

func TestCompareBPlusTreesSame(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	resourceMap := make(map[int64][]byte, 0)

	m1 := []byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // BeforeNodeOffset: -1
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m1 = append(m1, make([]uint8, pageSize-len(m1)-base.DataByteLengthOffset)...)
	m1 = append(m1, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0, // AfterNodeOffset: 2000
	}...)

	m2 := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, // BeforeNodeOffset: 1000
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m2 = append(m2, make([]uint8, pageSize-len(m2)-base.DataByteLengthOffset)...)
	m2 = append(m2, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0xb8, // AfterNodeOffset: 3000
	}...)

	m3 := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0, // BeforeNodeOffset: 2000
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "a"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m3 = append(m3, make([]uint8, pageSize-len(m3)-base.DataByteLengthOffset)...)
	m3 = append(m3, []byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // AfterNodeOffset: -1
	}...)

	// resourceMap放进去这些初始值
	resourceMap[1000] = m1
	resourceMap[2000] = m2
	resourceMap[3000] = m3

	// 创建两个相同的B+树
	tree1 := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("aa")},
				{Value: []byte("bb")},
			},
			KeysOffsetList:   []int64{1000, 2000, 3000},
			DataValues:       nil,
			Offset:           0,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    4 * 2,
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
		},
		LeafOrder:       1,
		IndexOrder:      1,
		ResourceManager: resource.InitMemoryConfig(resourceMap),
	}
	tree2 := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("aa")},
				{Value: []byte("bb")},
			},
			KeysOffsetList:   []int64{1000, 2000, 3000},
			DataValues:       nil,
			Offset:           0,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    4 * 2,
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
	if !isSame {
		t.Error("Expected true, but got false ")
	}

	tree3 := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("ab")},
				{Value: []byte("bb")},
			},
			KeysOffsetList:   []int64{1000, 2000, 3000},
			DataValues:       nil,
			Offset:           0,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    4 * 2,
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
		},
		LeafOrder:       1,
		IndexOrder:      1,
		ResourceManager: resource.InitMemoryConfig(resourceMap),
	}

	// 对比两个树，期望为 false
	isSame, err = tree1.CompareBPlusTreesSame(&tree3)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isSame {
		t.Error("Expected false, but got true ")
	}

	resourceMap2 := make(map[int64][]byte, 0)

	m4 := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0, // BeforeNodeOffset: 2000
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // node value length: 2
		0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "c"
		0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  name: "Alice"
		0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "20"
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id: "b"
		0x42, 0x6f, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name: "Bob"
		0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // age: "22"
	}
	m4 = append(m4, make([]uint8, pageSize-len(m4)-base.DataByteLengthOffset)...)
	m4 = append(m4, []byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // AfterNodeOffset: -1
	}...)

	// resourceMap放进去这些初始值
	resourceMap2[1000] = m1
	resourceMap2[2000] = m2
	resourceMap2[3000] = m4

	tree4 := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("aa")},
				{Value: []byte("bb")},
			},
			KeysOffsetList:   []int64{1000, 2000, 3000},
			DataValues:       nil,
			Offset:           0,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    4 * 2,
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
		},
		LeafOrder:       1,
		IndexOrder:      1,
		ResourceManager: resource.InitMemoryConfig(resourceMap2),
	}

	// 对比两个树，期望为 false
	isSame, err = tree1.CompareBPlusTreesSame(&tree4)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isSame {
		t.Error("Expected false, but got true ")
	}

	tree5 := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf: false,
			KeysValueList: []*ValueInfo{
				{Value: []byte("aa")},
				{Value: []byte("bb")},
			},
			KeysOffsetList:   []int64{1000, 2000, 3000},
			DataValues:       nil,
			Offset:           0,
			BeforeNodeOffset: 456,
			AfterNodeOffset:  789,
			ParentOffset:     0,
		},
		TableInfo: &tableSchema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
				Name:      "id",
				Length:    4 * 2,
				FieldType: tableSchema.StringType,
			},
			ValueFieldInfo: []*tableSchema.FieldInfo{
				{
					Name:      "name",
					Length:    4 * 5, // 假设最长5字
					FieldType: tableSchema.StringType,
				},
				{
					Name:      "age2",
					Length:    4 * 2, // 假设最长2字
					FieldType: tableSchema.StringType,
				},
			},
		},
		LeafOrder:       1,
		IndexOrder:      1,
		ResourceManager: resource.InitMemoryConfig(resourceMap),
	}

	// 对比两个树，期望为 false
	isSame, err = tree1.CompareBPlusTreesSame(&tree5)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isSame {
		t.Error("Expected false, but got true ")
	}
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

func TestBPlusTree_Insert(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	var err base.StandardError

	// 创建B+树
	tree := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf:           true,
			KeysValueList:    []*ValueInfo{},
			KeysOffsetList:   nil,
			DataValues:       []map[string]*ValueInfo{},
			Offset:           base.RootOffsetValue,
			BeforeNodeOffset: base.OffsetNull,
			AfterNodeOffset:  base.OffsetNull,
			ParentOffset:     base.OffsetNull,
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
					Length:    4 * 5, // 假设最长5字
					FieldType: tableSchema.StringType,
				},
				{
					Name:      "age",
					Length:    4 * 2, // 假设最长2字
					FieldType: tableSchema.StringType,
				},
			},
		},
		LeafOrder:       4,
		IndexOrder:      4,
		ResourceManager: resource.InitMemoryConfig(nil),
	}

	err = tree.Insert([]byte{}, [][]byte{})
	if err == nil {
		t.Error("Expected error, but got nil")
	}

	byteListKeyValue1 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	err = tree.Insert(byteListKeyValue1, [][]byte{})
	if err == nil {
		t.Error("Expected error, but got nil")
	}

	byteListDataValue1 := [][]byte{
		{0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "Alice"
		{0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "20"
	}
	err = tree.Insert([]byte{}, byteListDataValue1)
	if err == nil {
		t.Error("Expected error, but got nil")
	}

	err = tree.Insert(byteListKeyValue1, byteListDataValue1)
	if err != nil {
		t.Error("Expected error, but got nil")
	}
	err = tree.PrintBPlusTree()
	if err != nil {
		t.Error("Expected error, but got nil")
	}
}

func TestBPlusTree_Insert_2(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	var (
		err        base.StandardError
		jsonString string
		isSame     bool
	)

	// 创建B+树
	tree := BPlusTree{
		Root: &BPlusTreeNode{
			IsLeaf:           true,
			KeysValueList:    []*ValueInfo{},
			KeysOffsetList:   nil,
			DataValues:       []map[string]*ValueInfo{},
			Offset:           base.RootOffsetValue,
			BeforeNodeOffset: base.OffsetNull,
			AfterNodeOffset:  base.OffsetNull,
			ParentOffset:     base.OffsetNull,
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
					Length:    4 * 5, // 假设最长5字
					FieldType: tableSchema.StringType,
				},
				{
					Name:      "age",
					Length:    4 * 2, // 假设最长2字
					FieldType: tableSchema.StringType,
				},
			},
		},
		LeafOrder:       4,
		IndexOrder:      4,
		ResourceManager: resource.InitMemoryConfig(nil),
	}

	byteListKeyValue1 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

	byteListDataValue1 := [][]byte{
		{0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "Alice"
		{0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "20"
	}
	err = tree.Insert(byteListKeyValue1, byteListDataValue1)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	if tree.Root.IsLeaf != true {
		t.Errorf("Expected root is leaf, but got false")
	}
	rawJsonString2 := "{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"1\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree2, err := LoadBPlusTreeFromJson([]byte(rawJsonString2))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree2)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 1 pass")

	byteListDataValue2 := [][]byte{
		{0x61, 0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "aa"
		{0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "22"
	}
	byteListKeyValue2 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}
	err = tree.Insert(byteListKeyValue2, byteListDataValue2)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString3 := "{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree3, err := LoadBPlusTreeFromJson([]byte(rawJsonString3))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree3)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 2 pass")

	byteListDataValue3 := [][]byte{
		{0x61, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "ab"
		{0x32, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "23"
	}
	byteListKeyValue3 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03}
	err = tree.Insert(byteListKeyValue3, byteListDataValue3)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString4 := "{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"1\",\"2\",\"3\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"23\",\"name\":\"ab\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree4, err := LoadBPlusTreeFromJson([]byte(rawJsonString4))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree4)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 3 pass")

	byteListDataValue4 := [][]byte{
		{0x62, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "bb"
		{0x32, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "24"
	}
	byteListKeyValue4 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04}
	err = tree.Insert(byteListKeyValue4, byteListDataValue4)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString5 := "{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"3\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"parent_offset\":0,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"parent_offset\":0,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree5, err := LoadBPlusTreeFromJson([]byte(rawJsonString5))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree5)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 4 pass")

	byteListDataValue5 := [][]byte{
		{0x61, 0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "ac"
		{0x32, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "25"
	}
	byteListKeyValue5 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}
	err = tree.Insert(byteListKeyValue5, byteListDataValue5)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString6 := "{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"3\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"parent_offset\":0,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"parent_offset\":0,\"keys_value\":[\"3\",\"4\",\"5\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"25\",\"name\":\"ac\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree6, err := LoadBPlusTreeFromJson([]byte(rawJsonString6))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree6)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 5 pass")

	byteListDataValue6 := [][]byte{
		{0x63, 0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "cc"
		{0x32, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "26"
	}
	byteListKeyValue6 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06}
	err = tree.Insert(byteListKeyValue6, byteListDataValue6)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString7 := "{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"3\",\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"parent_offset\":0,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"parent_offset\":0,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"parent_offset\":0,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree7, err := LoadBPlusTreeFromJson([]byte(rawJsonString7))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree7)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 6 pass")

	byteListDataValue7 := [][]byte{
		{0x62, 0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "bc"
		{0x32, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "27"
	}
	byteListKeyValue7 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07}
	err = tree.Insert(byteListKeyValue7, byteListDataValue7)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString8 := "{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"3\",\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"parent_offset\":0,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"parent_offset\":0,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"parent_offset\":0,\"keys_value\":[\"5\",\"6\",\"7\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"27\",\"name\":\"bc\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree8, err := LoadBPlusTreeFromJson([]byte(rawJsonString8))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree8)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 7 pass")

	byteListDataValue8 := [][]byte{
		{0x63, 0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "ca"
		{0x32, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "28"
	}
	byteListKeyValue8 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}
	err = tree.Insert(byteListKeyValue8, byteListDataValue8)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString9 := "{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"7\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"parent_offset\":5000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"parent_offset\":5000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"parent_offset\":0,\"keys_value\":[\"3\",\"5\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"parent_offset\":0,\"keys_value\":[\"7\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"parent_offset\":6000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"parent_offset\":6000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree9, err := LoadBPlusTreeFromJson([]byte(rawJsonString9))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree9)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 8 pass")

	byteListDataValue9 := [][]byte{
		{0x63, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "cb"
		{0x32, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "29"
	}
	byteListKeyValue9 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09}
	err = tree.Insert(byteListKeyValue9, byteListDataValue9)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString10 := "{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"7\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"parent_offset\":0,\"keys_value\":[\"3\",\"5\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"parent_offset\":0,\"keys_value\":[\"7\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"parent_offset\":6000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"parent_offset\":6000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"parent_offset\":5000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"parent_offset\":5000,\"keys_value\":[\"7\",\"8\",\"9\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"29\",\"name\":\"cb\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree10, err := LoadBPlusTreeFromJson([]byte(rawJsonString10))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree10)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 9 pass")

	byteListDataValue10 := [][]byte{
		{0x62, 0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "ba"
		{0x33, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // age: "30"
	}
	byteListKeyValue10 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}
	err = tree.Insert(byteListKeyValue10, byteListDataValue10)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString11 := "{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"parent_offset\":-1,\"keys_value\":[\"7\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"parent_offset\":0,\"keys_value\":[\"7\",\"9\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"parent_offset\":6000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"parent_offset\":6000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"parent_offset\":5000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"parent_offset\":5000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"parent_offset\":5000,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"parent_offset\":0,\"keys_value\":[\"3\",\"5\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}]},\"leaf_order\":4,\"index_order\":4}"
	tree11, err := LoadBPlusTreeFromJson([]byte(rawJsonString11))
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	isSame, err = tree.CompareBPlusTreesSame(tree11)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}
	utils.LogDebug("Insert 10 pass")

	err = tree.PrintBPlusTree()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}

	jsonString, err = tree.BPlusTreeToJson()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	utils.LogDebug(jsonString)

	utils.LogDebug("Insert_2 all test pass")
}
