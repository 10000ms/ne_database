package core

import (
	"encoding/json"
	"fmt"

	"ne_database/core/resource"
	"ne_database/utils"
)

// BPlusTree B+树结构体
type BPlusTree struct {
	Root           *BPlusTreeNode   // 根节点
	Name           string           // B+树的名词，也是表名
	LeafOrder      int              // 叶子节点的B+树的阶数
	IndexOrder     int              // 非叶子节点的B+树的阶数
	ResourceConfig *resource.Config // 资源文件的获取方法
}

type BPlusTreeNode struct {
	IsLeaf           bool          `json:"is_leaf"`            // 是否为叶子节点
	KeysValueList    []int64       `json:"keys_value_list"`    // key的index，目前只支持int64的key，后面再变成interface{}
	KeysOffsetList   []int64       `json:"keys_offset_list"`   // index对应的子节点的offset列表，长度比KeysValueList +1，最后一个是尾部的offset
	DataValues       []interface{} `json:"data_values"`        // 值列表
	Offset           int64         `json:"offset"`             // 该节点在硬盘文件中的偏移量，也是该节点的id
	BeforeNodeOffset int64         `json:"before_node_offset"` // 该节点相连的前一个结点的偏移量
	AfterNodeOffset  int64         `json:"after_node_offset"`  // 该节点相连的后一个结点的偏移量
	ParentOffset     int64         `json:"parent_offset"`      // 该节点父结点偏移量
}

// LoadByteData 从[]byte数据中加载节点结构体
func (tree *BPlusTree) LoadByteData(offset int64, data []byte) *BPlusTreeNode {
	node := BPlusTreeNode{}
	node.Offset = offset
	// 1. 加载第一位，判断是否是叶子结点
	// 2. 加载这个节点的相邻两个节点的偏移量(offset)
	// 3. 加载这个节点的实际数据
	return &node
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

// PrintBPlusTree 这个方法按照层级分行打印出B+树的每个节点的键值，方便查看B+树的结构。
func (tree *BPlusTree) PrintBPlusTree() {
	utils.LogDebug("PrintBPlusTree")
	fmt.Printf("\n---**** PrintBPlusTree ****---\n")
	queue := make([]*BPlusTreeNode, 0) // 队列存放节点
	queue = append(queue, tree.Root)
	level := 0             // 当前节点所在的层数
	currentLevelCount := 1 // 当前层级节点数量
	nextLevelCount := 0    // 下一层级节点数量
	fmt.Printf("Level %d:\n", level)
	for len(queue) > 0 {
		node := queue[0]       // 取队列中的第一个节点
		queue = queue[1:]      // 从队列中删除第一个节点
		currentLevelCount -= 1 // 当前层级节点数量减1
		if node != nil {
			if node.IsLeaf == true {
				fmt.Printf("[leaf|%s|->%s], ", utils.ToJSON(node.Keys), utils.ToJSON(node.Values))
			} else {
				fmt.Printf("[%s->%s], ", utils.ToJSON(node.Keys), utils.ToJSON(node.Values))
			}
			if len(node.Child) > 0 {
				nextLevelCount += len(node.Child)
			}
			for _, child := range node.Child {
				queue = append(queue, child) // 将子节点加入队列中
			}
		}
		if currentLevelCount == 0 { // 当前层级节点输出完毕时，进入下一层级
			level++
			currentLevelCount = nextLevelCount
			nextLevelCount = 0
			fmt.Printf("\nLevel %d:\n", level)
		}
	}
	fmt.Printf("\n---**** END ****---\n")
	fmt.Printf("\n")
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
