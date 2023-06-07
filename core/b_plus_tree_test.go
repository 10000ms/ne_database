package core

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"ne_database/core/base"
	"ne_database/core/resource"
	tableSchema "ne_database/core/table_schema"
)

func TestGetNoLeafNodeByteDataReadLoopData(t *testing.T) {
	// 构造测试数据
	data := []byte{0, 0, 0, 1, 'a', 'b', 'c'}
	loopTime := 0 // 测试第一轮解析
	primaryKeyInfo := &tableSchema.FieldInfo{
		Name:      "id",
		Length:    3,
		FieldType: tableSchema.Int64Type,
	}

	// 调用被测试函数
	result, err := getNoLeafNodeByteDataReadLoopData(data, loopTime, primaryKeyInfo)

	// 断言结果
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result.Offset != 1 {
		t.Errorf("expected offset to be 1, but got %d", result.Offset)
	}
	if !result.OffsetSuccess {
		t.Errorf("expected offsetSuccess to be true, but got false")
	}
	if !result.PrimaryKeySuccess {
		t.Errorf("expected primaryKeySuccess to be true, but got false")
	}
	if string(result.PrimaryKey.Value) != "abc" {
		t.Errorf("expected primaryKey value to be 'abc', but got %q", result.PrimaryKey.Value)
	}
}

func TestBPlusTreeNode_NodeToByteData(t *testing.T) {
	// 初始化一下
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")
	pageSize := 1000
	_ = CoreConfig.InitByJSON(fmt.Sprintf("{\"Dev\":true,\"PageSize\":%d}", pageSize))

	tableInfo := &tableSchema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableSchema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableSchema.Int64Type,
		},
		ValueFieldInfo: []*tableSchema.FieldInfo{
			&tableSchema.FieldInfo{
				Name:      "name",
				Length:    4 * 20, // 假设最长20字节
				FieldType: tableSchema.StringType,
			},
			&tableSchema.FieldInfo{
				Name:      "age",
				Length:    4 * 5, // 假设最长20字节
				FieldType: tableSchema.StringType,
			},
		},
	}

	key1, err := base.Int64ToByteList(int64(1))
	if err != nil {
		t.Errorf("Int64ToByteList Error: %v", err)
	}

	key2, err := base.Int64ToByteList(int64(2))
	if err != nil {
		t.Errorf("Int64ToByteList Error: %v", err)
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

	data, err := node.NodeToByteData(tableInfo)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	expected := []byte{
		0x01,                                           // IsLeaf: true
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // BeforeNodeOffset: 50
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Offset of 'a'
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, // Offset of 'b'
		0x61,                         // 'a'
		0x41, 0x6c, 0x69, 0x63, 0x65, // "Alice"
		0x32, 0x30, // "20"
		0x62,             // 'b'
		0x42, 0x6f, 0x62, // "Bob"
		0x32, 0x32, // "22"

	}
	expected = append(expected, make([]uint8, pageSize-len(expected)-base.DataByteLengthOffset)...)
	expected = append(expected, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x96, // AfterNodeOffset: 150
	}...)
	if !bytes.Equal(data, expected) {
		t.Errorf("Expected: %v \n\t\t\t\t\t  but got: %v", expected, data)
	}

	// TODO 完成更多场景的单元测试
	//node = &BPlusTreeNode{
	//	IsLeaf:           false,
	//	KeysValueList:    []*ValueInfo{{Value: []byte("a")}, {Value: []byte("b")}},
	//	KeysOffsetList:   []int64{50, 100, 200},
	//	DataValues:       nil,
	//	Offset:           1000,
	//	BeforeNodeOffset: 500,
	//	AfterNodeOffset:  1500,
	//	ParentOffset:     2000,
	//}
	//data, err = node.NodeToByteData(tableInfo)
	//if err != nil {
	//	t.Errorf("Error: %v", err)
	//}
	//expected = []byte{
	//	0x00,                                           // IsLeaf: false
	//	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64, // BeforeNodeOffset: 500
	//	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // Offset of 'a'
	//	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // Offset of 'b'
	//	0x61,                                           // 'a'
	//	0x62,                                           // 'b'
	//	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc8, // Offset of a child node
	//	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Padding
	//}
	//
	//if !bytes.Equal(data, expected) {
	//	t.Errorf("Expected: %v, but got: %v", expected, data)
	//}
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
				&tableSchema.FieldInfo{
					Name:      "name",
					Length:    4 * 20, // 假设最长20字节
					FieldType: tableSchema.StringType,
				},
				&tableSchema.FieldInfo{
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
				&tableSchema.FieldInfo{
					Name:      "name",
					Length:    4 * 20, // 假设最长20字节
					FieldType: tableSchema.StringType,
				},
				&tableSchema.FieldInfo{
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
