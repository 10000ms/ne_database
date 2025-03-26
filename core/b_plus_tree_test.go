package core

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"ne_database/core/base"
	"ne_database/core/config"
	"ne_database/core/dataio"
	"ne_database/core/tableschema"
	"ne_database/utils"
	"ne_database/utils/list"
)

// TODO 完成两种储存类型的测试

var testStorageType = base.StorageTypeMemory

//var testStorageType = base.StorageTypeFile

func TestGetNoLeafNodeByteDataReadLoopData(t *testing.T) {
	// 初始化一下
	_ = os.Setenv("LOG_DEV", "1")
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
	primaryKeyInfo := &tableschema.FieldInfo{
		Name:      "id",
		Length:    4 * 2, // 假设最长2字
		FieldType: tableschema.CharType,
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
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
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

	tableInfo1 := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
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
	}

	targetNode := &BPlusTreeNode{}
	err = targetNode.LoadByteData(offset, tableInfo1, data)
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

	tableInfo2 := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    4 * 2, // 假设最长2字
			FieldType: tableschema.CharType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
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
	node = &BPlusTreeNode{
		IsLeaf:           false,
		KeysValueList:    []*ValueInfo{{Value: keyA}, {Value: keyB}},
		KeysOffsetList:   []int64{50, 100, 200},
		DataValues:       nil,
		Offset:           offset,
		BeforeNodeOffset: 500,
		AfterNodeOffset:  1500,
	}

	targetNode = &BPlusTreeNode{}
	err = targetNode.LoadByteData(offset, tableInfo2, data)
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
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	tableInfo1 := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
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

	tableInfo2 := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    4 * 2, // 假设最长2字
			FieldType: tableschema.CharType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
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
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")

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

	tableInfo1 := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
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
	}

	jsonNode, err := node.BPlusTreeNodeToJson(tableInfo1)
	if err != nil {
		t.Errorf("BPlusTreeNodeToJson Error: %v", err)
		return
	}
	utils.LogDebug(fmt.Sprintf("targetNode: %s", jsonNode))
	expJson := "{\"is_leaf\":true,\"keys_offset_list\":[0,8],\"offset\":100,\"before_node_offset\":50,\"after_node_offset\":150,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"Bob\"}]}"
	if jsonNode != expJson {
		t.Errorf("BPlusTreeNodeToJson no same expect: %s, get: %s", expJson, jsonNode)
		return
	}

}

func TestLoadBPlusTreeFromJson(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	dataMap := make(map[int64][]byte, 0)

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

	root := &BPlusTreeNode{
		IsLeaf: false,
		KeysValueList: []*ValueInfo{
			{Value: []byte("aa")},
			{Value: []byte("bb")},
		},
		KeysOffsetList:   []int64{1000, 2000, 3000},
		DataValues:       nil,
		Offset:           0,
		BeforeNodeOffset: base.OffsetNull,
		AfterNodeOffset:  base.OffsetNull,
	}
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    4 * 2,
			FieldType: tableschema.CharType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
	}
	rootByte, err := root.NodeToByteData(tableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// dataMap放进去这些初始值
	dataMap[base.RootOffsetValue] = rootByte
	dataMap[1000] = m1
	dataMap[2000] = m2
	dataMap[3000] = m3

	initManagerFunc, err := dataio.GetManagerInitFuncByType(testStorageType)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	dateManager, err := initManagerFunc(dataMap, config.CoreConfig.PageSize)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	defer dateManager.Close()

	// 创建B+树
	tree := BPlusTree{
		Root:        root,
		TableInfo:   tableInfo,
		LeafOrder:   1,
		IndexOrder:  1,
		DataManager: dateManager,
	}

	jsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[1000,2000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"aa\",\"bb\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":-1,\"after_node_offset\":2000,\"keys_value\":[\"a\",\"b\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"Bob\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":1000,\"after_node_offset\":3000,\"keys_value\":[\"a\",\"b\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"Bob\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"parent_offset\":0,\"keys_value\":[\"a\",\"b\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"Bob\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":1,\"index_order\":1}", testStorageType)

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
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	dataMap := make(map[int64][]byte, 0)

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

	root := &BPlusTreeNode{
		IsLeaf: false,
		KeysValueList: []*ValueInfo{
			{Value: []byte("aa")},
			{Value: []byte("bb")},
		},
		KeysOffsetList:   []int64{1000, 2000, 3000},
		DataValues:       nil,
		Offset:           0,
		BeforeNodeOffset: base.OffsetNull,
		AfterNodeOffset:  base.OffsetNull,
	}
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    4 * 2,
			FieldType: tableschema.CharType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
	}
	rootByte, err := root.NodeToByteData(tableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// dataMap放进去这些初始值
	dataMap[base.RootOffsetValue] = rootByte
	dataMap[1000] = m1
	dataMap[2000] = m2
	dataMap[3000] = m3

	initManagerFunc, err := dataio.GetManagerInitFuncByType(testStorageType)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	dateManager, err := initManagerFunc(dataMap, config.CoreConfig.PageSize)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	defer dateManager.Close()

	// 创建B+树
	tree := BPlusTree{
		Root:        root,
		TableInfo:   tableInfo,
		LeafOrder:   1,
		IndexOrder:  1,
		DataManager: dateManager,
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
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	dataMap := make(map[int64][]byte)
	dataMap2 := make(map[int64][]byte)

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

	root := &BPlusTreeNode{
		IsLeaf: false,
		KeysValueList: []*ValueInfo{
			{Value: []byte("aa")},
			{Value: []byte("bb")},
		},
		KeysOffsetList:   []int64{1000, 2000, 3000},
		DataValues:       nil,
		Offset:           0,
		BeforeNodeOffset: base.OffsetNull,
		AfterNodeOffset:  base.OffsetNull,
	}
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    4 * 2,
			FieldType: tableschema.CharType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
	}
	rootByte, err := root.NodeToByteData(tableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// dataMap放进去这些初始值
	dataMap[base.RootOffsetValue] = rootByte
	dataMap[1000] = m1
	dataMap[2000] = m2
	dataMap[3000] = m3

	initManagerFunc, err := dataio.GetManagerInitFuncByType(testStorageType)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	dateManager, err := initManagerFunc(dataMap, config.CoreConfig.PageSize)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	defer dateManager.Close()

	// 创建两个相同的B+树
	tree1 := BPlusTree{
		Root:        root,
		TableInfo:   tableInfo,
		LeafOrder:   1,
		IndexOrder:  1,
		DataManager: dateManager,
	}
	tree2 := BPlusTree{
		Root:        root,
		TableInfo:   tableInfo,
		LeafOrder:   1,
		IndexOrder:  1,
		DataManager: dateManager,
	}

	// 对比两个树，期望为true
	isSame, err := tree1.CompareBPlusTreesSame(&tree2)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if !isSame {
		t.Error("Expected true, but got false ")
	}

	root2 := &BPlusTreeNode{
		IsLeaf: false,
		KeysValueList: []*ValueInfo{
			{Value: []byte("ab")},
			{Value: []byte("bb")},
		},
		KeysOffsetList:   []int64{1000, 2000, 3000},
		DataValues:       nil,
		Offset:           0,
		BeforeNodeOffset: base.OffsetNull,
		AfterNodeOffset:  base.OffsetNull,
	}
	rootByte2, err := root2.NodeToByteData(tableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// dataMap放进去这些初始值
	dataMap2[base.RootOffsetValue] = rootByte2
	dataMap2[1000] = m1
	dataMap2[2000] = m2
	dataMap2[3000] = m3

	dateManager2, err := initManagerFunc(dataMap, config.CoreConfig.PageSize)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	defer dateManager2.Close()

	tree3 := BPlusTree{
		Root:        root2,
		TableInfo:   tableInfo,
		LeafOrder:   1,
		IndexOrder:  1,
		DataManager: dateManager2,
	}

	// 对比两个树，期望为 false
	isSame, err = tree1.CompareBPlusTreesSame(&tree3)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isSame {
		t.Error("Expected false, but got true ")
		return
	}

	dataMap3 := make(map[int64][]byte, 0)

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

	// dataMap放进去这些初始值
	dataMap3[base.RootOffsetValue] = rootByte
	dataMap3[1000] = m1
	dataMap3[2000] = m2
	dataMap3[3000] = m4

	dateManager3, err := initManagerFunc(dataMap3, config.CoreConfig.PageSize)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	defer dateManager3.Close()

	tree4 := BPlusTree{
		Root:        root,
		TableInfo:   tableInfo,
		LeafOrder:   1,
		IndexOrder:  1,
		DataManager: dateManager3,
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
		Root: root,
		TableInfo: &tableschema.TableMetaInfo{
			Name: "users",
			PrimaryKeyFieldInfo: &tableschema.FieldInfo{
				Name:      "id",
				Length:    4 * 2,
				FieldType: tableschema.CharType,
			},
			ValueFieldInfo: []*tableschema.FieldInfo{
				{
					Name:      "name",
					Length:    4 * 5, // 假设最长5字
					FieldType: tableschema.CharType,
				},
				{
					Name:      "age2",
					Length:    4 * 2, // 假设最长2字
					FieldType: tableschema.CharType,
				},
			},
			PageSize:    config.CoreConfig.PageSize,
			StorageType: testStorageType,
		},
		LeafOrder:   1,
		IndexOrder:  1,
		DataManager: dateManager,
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
	}

	isEqual, err = node1.CompareBPlusTreeNodesSame(&node4)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
	}

	node5 := BPlusTreeNode{
		IsLeaf: false,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList:   []int64{100, 200, 301},
		DataValues:       nil,
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
	}

	node11 := BPlusTreeNode{
		IsLeaf: false,
		KeysValueList: []*ValueInfo{
			{Value: []byte("hello")},
			{Value: []byte("world")},
		},
		KeysOffsetList:   []int64{100, 200, 300},
		DataValues:       nil,
		Offset:           123,
		BeforeNodeOffset: 456,
		AfterNodeOffset:  789,
	}

	isEqual, err = node11.CompareBPlusTreeNodesSame(&node5)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	if isEqual {
		t.Error("Expected false, but got true ")
		return
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
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	var err base.StandardError

	dataMap := make(map[int64][]byte)
	root := &BPlusTreeNode{
		IsLeaf:           true,
		KeysValueList:    []*ValueInfo{},
		KeysOffsetList:   nil,
		DataValues:       []map[string]*ValueInfo{},
		Offset:           base.RootOffsetValue,
		BeforeNodeOffset: base.OffsetNull,
		AfterNodeOffset:  base.OffsetNull,
	}
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
	}
	rootByte, err := root.NodeToByteData(tableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// dataMap放进去这些初始值
	dataMap[base.RootOffsetValue] = rootByte

	initManagerFunc, err := dataio.GetManagerInitFuncByType(testStorageType)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	dateManager, err := initManagerFunc(dataMap, config.CoreConfig.PageSize)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	defer dateManager.Close()

	// 创建B+树
	tree := BPlusTree{
		Root:        root,
		TableInfo:   tableInfo,
		LeafOrder:   4,
		IndexOrder:  4,
		DataManager: dateManager,
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
		{0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "20"
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
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	var (
		err        base.StandardError
		jsonString string
		isSame     bool
	)

	dataMap := make(map[int64][]byte)
	root := &BPlusTreeNode{
		IsLeaf:           true,
		KeysValueList:    []*ValueInfo{},
		KeysOffsetList:   nil,
		DataValues:       []map[string]*ValueInfo{},
		Offset:           base.RootOffsetValue,
		BeforeNodeOffset: base.OffsetNull,
		AfterNodeOffset:  base.OffsetNull,
	}
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
	}
	rootByte, err := root.NodeToByteData(tableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// dataMap放进去这些初始值
	dataMap[base.RootOffsetValue] = rootByte

	initManagerFunc, err := dataio.GetManagerInitFuncByType(testStorageType)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	dateManager, err := initManagerFunc(dataMap, config.CoreConfig.PageSize)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	defer dateManager.Close()

	// 创建B+树
	tree := BPlusTree{
		Root:        root,
		TableInfo:   tableInfo,
		LeafOrder:   4,
		IndexOrder:  4,
		DataManager: dateManager,
	}

	byteListKeyValue1 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

	byteListDataValue1 := [][]byte{
		{0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "Alice"
		{0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "20"
	}
	err = tree.Insert(byteListKeyValue1, byteListDataValue1)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	if tree.Root.IsLeaf != true {
		t.Errorf("Expected root is leaf, but got false")
	}
	rawJsonString2 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"1\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree2, err := LoadBPlusTreeFromJson([]byte(rawJsonString2))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
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
		{0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "22"
	}
	byteListKeyValue2 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}
	err = tree.Insert(byteListKeyValue2, byteListDataValue2)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString3 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree3, err := LoadBPlusTreeFromJson([]byte(rawJsonString3))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
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
		{0x32, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "23"
	}
	byteListKeyValue3 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03}
	err = tree.Insert(byteListKeyValue3, byteListDataValue3)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString4 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"1\",\"2\",\"3\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"23\",\"name\":\"ab\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "24"
	}
	byteListKeyValue4 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04}
	err = tree.Insert(byteListKeyValue4, byteListDataValue4)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString5 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"2\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "25"
	}
	byteListKeyValue5 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}
	err = tree.Insert(byteListKeyValue5, byteListDataValue5)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString6 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"2\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"keys_value\":[\"3\",\"4\",\"5\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"25\",\"name\":\"ac\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "26"
	}
	byteListKeyValue6 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06}
	err = tree.Insert(byteListKeyValue6, byteListDataValue6)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString7 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "27"
	}
	byteListKeyValue7 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07}
	err = tree.Insert(byteListKeyValue7, byteListDataValue7)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString8 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"6\",\"7\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"27\",\"name\":\"bc\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "28"
	}
	byteListKeyValue8 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}
	err = tree.Insert(byteListKeyValue8, byteListDataValue8)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString9 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "29"
	}
	byteListKeyValue9 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09}
	err = tree.Insert(byteListKeyValue9, byteListDataValue9)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString10 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"7\",\"8\",\"9\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"29\",\"name\":\"cb\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x33, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "30"
	}
	byteListKeyValue10 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}
	err = tree.Insert(byteListKeyValue10, byteListDataValue10)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString11 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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

func TestBPlusTree_Insert_3(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	var (
		err        base.StandardError
		jsonString string
		isSame     bool
	)

	dataMap := make(map[int64][]byte)
	root := &BPlusTreeNode{
		IsLeaf:           true,
		KeysValueList:    []*ValueInfo{},
		KeysOffsetList:   nil,
		DataValues:       []map[string]*ValueInfo{},
		Offset:           base.RootOffsetValue,
		BeforeNodeOffset: base.OffsetNull,
		AfterNodeOffset:  base.OffsetNull,
	}
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: tableschema.CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: testStorageType,
	}
	rootByte, err := root.NodeToByteData(tableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}

	// dataMap放进去这些初始值
	dataMap[base.RootOffsetValue] = rootByte

	initManagerFunc, err := dataio.GetManagerInitFuncByType(testStorageType)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	dateManager, err := initManagerFunc(dataMap, config.CoreConfig.PageSize)
	if err != nil {
		t.Error("Expected nil error, but got error")
	}
	defer dateManager.Close()

	// 创建B+树
	tree := BPlusTree{
		Root:        root,
		TableInfo:   tableInfo,
		LeafOrder:   4,
		IndexOrder:  4,
		DataManager: dateManager,
	}

	byteListKeyValue1 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03} // 3

	byteListDataValue1 := [][]byte{
		{0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //  name: "Alice"
		{0x32, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "20"
	}
	err = tree.Insert(byteListKeyValue1, byteListDataValue1)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	if tree.Root.IsLeaf != true {
		t.Errorf("Expected root is leaf, but got false")
	}
	rawJsonString2 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "22"
	}
	byteListKeyValue2 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04} // 4
	err = tree.Insert(byteListKeyValue2, byteListDataValue2)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString3 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "23"
	}
	byteListKeyValue3 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05} // 5
	err = tree.Insert(byteListKeyValue3, byteListDataValue3)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString4 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\",\"4\",\"5\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"23\",\"name\":\"ab\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "24"
	}
	byteListKeyValue4 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03} // 3
	err = tree.Insert(byteListKeyValue4, byteListDataValue4)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString5 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\"],\"data_values\":[{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"keys_value\":[\"4\",\"5\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "25"
	}
	byteListKeyValue5 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04} // 4
	err = tree.Insert(byteListKeyValue5, byteListDataValue5)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString6 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\"],\"data_values\":[{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"keys_value\":[\"4\",\"4\",\"5\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "26"
	}
	byteListKeyValue6 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05} // 5
	err = tree.Insert(byteListKeyValue6, byteListDataValue6)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString7 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\"],\"data_values\":[{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "27"
	}
	byteListKeyValue7 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03} // 3
	err = tree.Insert(byteListKeyValue7, byteListDataValue7)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString8 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "28"
	}
	byteListKeyValue8 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04} // 4
	err = tree.Insert(byteListKeyValue8, byteListDataValue8)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString9 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\",\"4\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x32, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "29"
	}
	byteListKeyValue9 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05} // 5
	err = tree.Insert(byteListKeyValue9, byteListDataValue9)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString10 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\",\"4\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\",\"5\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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
		{0x33, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},                                                                         // age: "30"
	}
	byteListKeyValue10 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05} // 5
	err = tree.Insert(byteListKeyValue10, byteListDataValue10)
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	rawJsonString11 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\",\"4\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"30\",\"name\":\"ba\"},{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
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

	utils.LogDebug("Insert_3 all test pass")
}

func TestBPlusTree_NodeParentMap(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[2000,3000,1000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[1000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"4\",\"4\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":3000,\"after_node_offset\":4000,\"keys_value\":[\"4\",\"5\",\"5\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"30\",\"name\":\"ba\"},{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	m, err := tree.NodeParentMap()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	for _, pInfo := range m {
		if pInfo.RightParent != base.OffsetNull && pInfo.LeftParent == base.OffsetNull {
			t.Error("Expected right and left both has value")
			return
		}
		if pInfo.LeftParent != base.OffsetNull && pInfo.RightParent == base.OffsetNull {
			t.Error("Expected right and left both has value")
			return
		}
		if pInfo.LeftParent == base.OffsetNull && pInfo.RightParent == base.OffsetNull && pInfo.OnlyParent == base.OffsetNull {
			t.Error("Expected has value")
			return
		}
		if pInfo.OnlyParent != base.OffsetNull && (pInfo.LeftParent != base.OffsetNull || pInfo.RightParent != base.OffsetNull) {
			t.Error("Expected Only only has value")
			return
		}
	}
	if !(m[1000].LeftParent == 5000 && m[1000].OnlyParent == base.OffsetNull && m[1000].RightParent == 6000) {
		t.Error("value error")
		return
	}
	if !(m[2000].LeftParent == base.OffsetNull && m[2000].OnlyParent == 6000 && m[2000].RightParent == base.OffsetNull) {
		t.Error("value error")
		return
	}
	if !(m[5000].LeftParent == base.OffsetNull && m[5000].OnlyParent == base.RootOffsetValue && m[5000].RightParent == base.OffsetNull) {
		t.Error("value error")
		return
	}
	utils.LogDebug(utils.ToJSON(m))
}

func TestBPlusTreeNode_IndexNodeClear(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"keys_value\":[],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err := tree.Root.IndexNodeClear(2000, tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 0 {
		t.Error("Expected 0")
		return
	}
	if hasFirstChange != true {
		t.Error("Expected true")
		return
	}
	if hasLastChange != false {
		t.Error("Expected false")
		return
	}

	rawJsonString2 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\",\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"keys_value\":[],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree2, err := LoadBPlusTreeFromJson([]byte(rawJsonString2))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err = tree2.Root.IndexNodeClear(1000, tree2)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 2 {
		t.Error("Expected 2")
		return
	}
	if hasFirstChange != false {
		t.Error("Expected false")
		return
	}
	if hasLastChange != false {
		t.Error("Expected false")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err = tree2.Root.IndexNodeClear(3000, tree2)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 0 {
		t.Error("Expected 0")
		return
	}
	if hasFirstChange != false {
		t.Error("Expected false")
		return
	}
	if hasLastChange != true {
		t.Error("Expected true")
		return
	}

	rawJsonString3 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\",\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":-1,\"keys_value\":[],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree3, err := LoadBPlusTreeFromJson([]byte(rawJsonString3))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err = tree3.Root.IndexNodeClear(1000, tree3)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 2 {
		t.Error("Expected 2")
		return
	}
	if hasFirstChange != false {
		t.Error("Expected false")
		return
	}
	if hasLastChange != false {
		t.Error("Expected false")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err = tree3.Root.IndexNodeClear(2000, tree3)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 0 {
		t.Error("Expected 0")
		return
	}
	if hasFirstChange != true {
		t.Error("Expected true")
		return
	}
	if hasLastChange != false {
		t.Error("Expected false")
		return
	}

	rawJsonString4 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\",\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree4, err := LoadBPlusTreeFromJson([]byte(rawJsonString4))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err = tree4.Root.IndexNodeClear(2000, tree4)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 2 {
		t.Error("Expected 2")
		return
	}
	if hasFirstChange != true {
		t.Error("Expected true")
		return
	}
	if hasLastChange != false {
		t.Error("Expected false")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err = tree4.Root.IndexNodeClear(1000, tree4)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 0 {
		t.Error("Expected 0")
		return
	}
	if hasFirstChange != true {
		t.Error("Expected true")
		return
	}
	if hasLastChange != false {
		t.Error("Expected false")
		return
	}

	rawJsonString5 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\",\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree5, err := LoadBPlusTreeFromJson([]byte(rawJsonString5))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err = tree5.Root.IndexNodeClear(2000, tree5)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 2 {
		t.Error("Expected 2")
		return
	}
	if hasFirstChange != true {
		t.Error("Expected true")
		return
	}
	if hasLastChange != false {
		t.Error("Expected false")
		return
	}
	remainItem, hasFirstChange, hasLastChange, err = tree5.Root.IndexNodeClear(3000, tree5)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 0 {
		t.Error("Expected 0")
		return
	}
	if hasFirstChange != false {
		t.Error("Expected false")
		return
	}
	if hasLastChange != true {
		t.Error("Expected true")
		return
	}
}

func TestBPlusTreeNode_LeafNodeClear(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":[],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\",\"5\",\"5\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.Int64ToByteList(2000)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, leftCheck, rightCheck, err := tree.Root.LeafNodeClear(keyValueByte, tree.TableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 3 {
		t.Error("Expected 3")
		return
	}
	if leftCheck != false {
		t.Error("Expected false")
		return
	}
	if rightCheck != false {
		t.Error("Expected false")
		return
	}
	keyValueByte, err = base.StringToByteList("4")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, leftCheck, rightCheck, err = tree.Root.LeafNodeClear(keyValueByte, tree.TableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 2 {
		t.Error("Expected 2")
		return
	}
	if leftCheck != true {
		t.Error("Expected true")
		return
	}
	if rightCheck != false {
		t.Error("Expected false")
		return
	}
	keyValueByte, err = base.StringToByteList("5")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, leftCheck, rightCheck, err = tree.Root.LeafNodeClear(keyValueByte, tree.TableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 0 {
		t.Error("Expected 0")
		return
	}
	if leftCheck != true {
		t.Error("Expected true")
		return
	}
	if rightCheck != true {
		t.Error("Expected true")
		return
	}

	rawJsonString2 := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":[],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\",\"5\",\"5\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree2, err := LoadBPlusTreeFromJson([]byte(rawJsonString2))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err = base.StringToByteList("5")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, leftCheck, rightCheck, err = tree2.Root.LeafNodeClear(keyValueByte, tree2.TableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 1 {
		t.Error("Expected 1")
		return
	}
	if leftCheck != false {
		t.Error("Expected false")
		return
	}
	if rightCheck != true {
		t.Error("Expected true")
		return
	}
	keyValueByte, err = base.StringToByteList("4")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	remainItem, leftCheck, rightCheck, err = tree2.Root.LeafNodeClear(keyValueByte, tree2.TableInfo)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if remainItem != 0 {
		t.Error("Expected 1")
		return
	}
	if leftCheck != true {
		t.Error("Expected true")
		return
	}
	if rightCheck != true {
		t.Error("Expected true")
		return
	}
}

func TestBPlusTree_Delete_1(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	var jsonString string

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[2000,3000,1000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[1000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"4\",\"4\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":3000,\"after_node_offset\":4000,\"keys_value\":[\"4\",\"5\",\"5\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"},{\"age\":\"30\",\"name\":\"ba\"},{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":1000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.Int64ToByteList(2000)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}

	testTree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err := testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}

	jsonString, err = tree.BPlusTreeToJson()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	utils.LogDebug(jsonString)

	utils.LogDebug("Delete_1 test pass")

}

func TestBPlusTree_Delete_2(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	var jsonString string

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\",\"4\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"30\",\"name\":\"ba\"},{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.StringToByteList("3")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}

	testJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\",\"4\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"30\",\"name\":\"ba\"},{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err := LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err := testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}

	keyValueByte, err = base.StringToByteList("4")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":-1,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"30\",\"name\":\"ba\"},{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}

	keyValueByte, err = base.StringToByteList("5")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[],\"data_values\":[]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}

	jsonString, err = tree.BPlusTreeToJson()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	utils.LogDebug(jsonString)

	utils.LogDebug("Delete_2 test pass")

}

func TestBPlusTree_Delete_3(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.StringToByteList("5")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}

	testJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[],\"data_values\":[]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err := LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err := testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	jsonString, err := tree.BPlusTreeToJson()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	utils.LogDebug(jsonString)

	utils.LogDebug("Delete_3 test pass")
}

func TestBPlusTree_Delete_4(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	var jsonString string

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}

	keyValueByte, err := base.Int64ToByteList(3)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\"],\"data_values\":[{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err := LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err := testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(4)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(1)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"2\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(10)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"2\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(5)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"2\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":4000,\"keys_value\":[\"6\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(7)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"2\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":4000,\"keys_value\":[\"6\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"8\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(8)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[3000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":3000,\"keys_value\":[\"2\"],\"data_values\":[{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":2000,\"after_node_offset\":7000,\"keys_value\":[\"6\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"9\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(2)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[3000,7000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"6\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":-1,\"after_node_offset\":7000,\"keys_value\":[\"6\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"9\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"}]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(6)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"9\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"}]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	keyValueByte, err = base.Int64ToByteList(9)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	err = tree.Delete(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	testJsonString = fmt.Sprintf("{\"root_node\":{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[],\"data_values\":[]},\"value_node\":[],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err = LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err = testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got false ")
		return
	}

	jsonString, err = tree.BPlusTreeToJson()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	utils.LogDebug(jsonString)

	utils.LogDebug("Delete_4 test pass")

}

func TestBPlusTree_Update_1(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\",\"4\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"30\",\"name\":\"ba\"},{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.StringToByteList("5")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	value, err := base.StringToByteList("50")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	values := make(map[string][]byte)
	values["age"] = value

	err = tree.Update(keyValueByte, values)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}

	testJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\",\"4\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"50\",\"name\":\"ba\"},{\"age\":\"50\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"50\",\"name\":\"cc\"},{\"age\":\"50\",\"name\":\"ab\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err := LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err := testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}

	jsonString, err := tree.BPlusTreeToJson()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	utils.LogDebug(jsonString)

	utils.LogDebug("Update_1 test pass")

}

func TestBPlusTree_Update_2(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.Int64ToByteList(5)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	value, err := base.StringToByteList("50")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	values := make(map[string][]byte)
	values["age"] = value

	err = tree.Update(keyValueByte, values)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}

	testJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"50\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	testTree, err := LoadBPlusTreeFromJson([]byte(testJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	isSame, err := testTree.CompareBPlusTreesSame(tree)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !isSame {
		t.Error("Expected false, but got true ")
		return
	}

	jsonString, err := tree.BPlusTreeToJson()
	if err != nil {
		t.Error("Expected error, but got nil")
		return
	}
	utils.LogDebug(jsonString)

	utils.LogDebug("Update_2 test pass")

}

func TestBPlusTree_Update_3(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.Int64ToByteList(5)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	value, err := base.StringToByteList("50")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	values := make(map[string][]byte)
	values["age1"] = value

	err = tree.Update(keyValueByte, values)
	if err == nil {
		t.Error("Expected error, but nil error")
		return
	}

	utils.LogDebug("Update_3 test pass")

}

func TestBPlusTree_SearchEqualKey_1(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"1\",\"2\"],\"data_values\":[{\"age\":\"20\",\"name\":\"Alice\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[{\"age\":\"23\",\"name\":\"ab\"},{\"age\":\"24\",\"name\":\"bb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"6\"],\"data_values\":[{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"26\",\"name\":\"cc\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":7000,\"keys_value\":[\"7\",\"8\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"28\",\"name\":\"ca\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":7000,\"before_node_offset\":4000,\"after_node_offset\":-1,\"keys_value\":[\"9\",\"10\"],\"data_values\":[{\"age\":\"29\",\"name\":\"cb\"},{\"age\":\"30\",\"name\":\"ba\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"2\",\"4\"],\"data_values\":[]},{\"is_leaf\":false,\"keys_offset_list\":[3000,4000,7000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"6\",\"8\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"int64\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.Int64ToByteList(5)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyList, valueList, err := tree.SearchEqualKey(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if len(keyList) != 1 {
		t.Error("Expected len(keyList) == 1, but no got")
		return
	}
	if len(valueList) != 1 {
		t.Error("Expected len(valueList) == 1, but no got")
		return
	}
	if !list.ByteListEqual(keyList[0], keyValueByte) {
		t.Error("value error")
		return
	}
	ageByte, err := base.StringToByteList("25")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	nameByte, err := base.StringToByteList("ac")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if !list.ByteListEqual(valueList[0]["age"], ageByte) {
		t.Error("value error")
		return
	}
	if !list.ByteListEqual(valueList[0]["name"], nameByte) {
		t.Error("value error")
		return
	}

}

func TestBPlusTree_SearchEqualKey_2(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = config.CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	rawJsonString := fmt.Sprintf("{\"root_node\":{\"is_leaf\":false,\"keys_offset_list\":[6000,5000],\"offset\":0,\"before_node_offset\":-1,\"after_node_offset\":-1,\"keys_value\":[\"4\"],\"data_values\":[]},\"value_node\":[{\"is_leaf\":false,\"keys_offset_list\":[3000,4000],\"offset\":5000,\"before_node_offset\":6000,\"after_node_offset\":-1,\"keys_value\":[\"5\"],\"data_values\":[]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":2000,\"before_node_offset\":-1,\"after_node_offset\":1000,\"keys_value\":[\"3\",\"3\",\"3\"],\"data_values\":[{\"age\":\"27\",\"name\":\"bc\"},{\"age\":\"24\",\"name\":\"bb\"},{\"age\":\"20\",\"name\":\"Alice\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":1000,\"before_node_offset\":2000,\"after_node_offset\":3000,\"keys_value\":[\"4\",\"4\",\"4\"],\"data_values\":[{\"age\":\"28\",\"name\":\"ca\"},{\"age\":\"25\",\"name\":\"ac\"},{\"age\":\"22\",\"name\":\"aa\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":3000,\"before_node_offset\":1000,\"after_node_offset\":4000,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"30\",\"name\":\"ba\"},{\"age\":\"29\",\"name\":\"cb\"}]},{\"is_leaf\":true,\"keys_offset_list\":null,\"offset\":4000,\"before_node_offset\":3000,\"after_node_offset\":-1,\"keys_value\":[\"5\",\"5\"],\"data_values\":[{\"age\":\"26\",\"name\":\"cc\"},{\"age\":\"23\",\"name\":\"ab\"}]},{\"is_leaf\":false,\"keys_offset_list\":[2000,1000,3000],\"offset\":6000,\"before_node_offset\":-1,\"after_node_offset\":5000,\"keys_value\":[\"3\",\"4\"],\"data_values\":[]}],\"table_info\":{\"name\":\"users\",\"primary_key\":{\"name\":\"id\",\"length\":8,\"default\":\"\",\"type\":\"string\"},\"value\":[{\"name\":\"name\",\"length\":20,\"default\":\"\",\"type\":\"string\"},{\"name\":\"age\",\"length\":8,\"default\":\"\",\"type\":\"string\"}],\"page_size\":1000,\"storage_type\":\"%s\"},\"leaf_order\":4,\"index_order\":4}", testStorageType)
	tree, err := LoadBPlusTreeFromJson([]byte(rawJsonString))
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyValueByte, err := base.StringToByteList("5")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	keyList, valueList, err := tree.SearchEqualKey(keyValueByte)
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	if len(keyList) != 4 {
		t.Error("Expected len(keyList) == 4, but no got")
		return
	}
	if len(valueList) != 4 {
		t.Error("Expected len(valueList) == 4, but no got")
		return
	}
	if !list.ByteListEqual(keyList[0], keyValueByte) {
		t.Error("value error")
		return
	}

	for _, l := range keyList {
		if l == nil {
			t.Error("Expected not nil, but got nil")
			return
		}
		if !list.ByteListEqual(l, keyValueByte) {
			t.Error("value error")
			return
		}

	}

	ageByte1, err := base.StringToByteList("23")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	ageByte2, err := base.StringToByteList("26")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	ageByte3, err := base.StringToByteList("29")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	ageByte4, err := base.StringToByteList("30")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	nameByte1, err := base.StringToByteList("ab")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	nameByte2, err := base.StringToByteList("cc")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	nameByte3, err := base.StringToByteList("ba")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}
	nameByte4, err := base.StringToByteList("cb")
	if err != nil {
		t.Error("Expected nil error, but got error")
		return
	}

	for _, value := range valueList {
		if value == nil {
			t.Error("Expected not nil, but got nil")
			return
		}
		for k, v := range value {
			if k != "age" && k != "name" {
				t.Error("value error")
				return
			}
			if k == "age" {
				if !list.ByteListEqual(v, ageByte1) && !list.ByteListEqual(v, ageByte2) && !list.ByteListEqual(v, ageByte3) && !list.ByteListEqual(v, ageByte4) {
					t.Error("value error")
					return
				}
			} else if k == "name" {
				if !list.ByteListEqual(v, nameByte1) && !list.ByteListEqual(v, nameByte2) && !list.ByteListEqual(v, nameByte3) && !list.ByteListEqual(v, nameByte4) {
					t.Error("value error")
					return
				}
			}
		}
	}
}
