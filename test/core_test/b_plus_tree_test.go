package core_test

import (
	"ne_database/utils"
	"testing"

	"ne_database/core"
)

func TestJsonToBPlusTree(t *testing.T) {
	jsonData := []byte(`
        {
            "is_leaf": false,
            "keys": [10, 25, 50],
            "values": [],
            "child": [
                {
                    "is_leaf": true,
                    "keys": [5, 8],
                    "values": ["apple", "banana"]
                },
                {
                    "is_leaf": true,
                    "keys": [10, 12, 15],
                    "values": ["grape", "orange", "peach"]
                },
                {
                    "is_leaf": true,
                    "keys": [25, 30],
                    "values": ["pineapple", "watermelon"]
                },
                {
                    "is_leaf": true,
                    "keys": [50, 80],
                    "values": ["avocado", "lemon"]
                }
            ]
        }
    `)
	root, err := core.JsonToBPlusTree(jsonData)
	if err != nil {
		t.Errorf("JsonToBPlusTree failed: %v", err)
	}
	if utils.ToJSON(root.Keys) != utils.ToJSON([]int{10, 25, 50}) {
		t.Errorf("JsonToBPlusTree failed: root.keys=%v", root.Keys)
	}
}

func TestJsonToBPlusTree2(t *testing.T) {
	jsonData := []byte(`
		{
		  "is_leaf": false,
		  "keys": [
			1
		  ],
		  "values": [],
		  "child": [
			{
			  "is_leaf": true,
			  "keys": [
				1
			  ],
			  "values": [
				{
				  "name": "apple",
				  "price": 2.5
				}
			  ],
			  "child": []
			}
		  ]
		}
		`)
	expectedNode := &core.BPlusTreeNode{
		IsLeaf: false,
		Keys:   []int64{1},
		Values: []interface{}{},
		Child: []*core.BPlusTreeNode{
			{
				IsLeaf: true,
				Keys:   []int64{1},
				Values: []interface{}{
					map[string]interface{}{
						"name":  "apple",
						"price": 2.5,
					},
				},
				Child:  nil,
				Parent: nil,
			},
		},
		Parent: nil,
	}
	// 设置Parent
	for _, n := range expectedNode.Child {
		n.Parent = expectedNode
	}

	actualNode, err := core.JsonToBPlusTree(jsonData)
	if err != nil {
		t.Fatal(err)
	}

	if !actualNode.CompareBPlusTreeNodes(expectedNode) {
		t.Errorf("Expected node %#v,\nbut got node %#v", expectedNode, actualNode)
	} else {
		tree := core.BPlusTree{
			Root: actualNode,
		}
		tree.PrintBPlusTree()
	}
}

func TestLoadBPlusTreeFromJson(t *testing.T) {
	jsonData := []byte(`
		{
		  "is_leaf": false,
		  "keys": [
			1
		  ],
		  "values": [],
		  "child": [
			{
			  "is_leaf": true,
			  "keys": [
				1
			  ],
			  "values": [
				{
				  "name": "apple",
				  "price": 2.5
				}
			  ],
			  "child": null
			}
		  ]
		}
		`)
	expectedTree := &core.BPlusTree{
		Root: &core.BPlusTreeNode{
			IsLeaf: false,
			Keys:   []int64{1},
			Values: []interface{}{},
			Child: []*core.BPlusTreeNode{
				{
					IsLeaf: true,
					Keys:   []int64{1},
					Values: []interface{}{
						map[string]interface{}{
							"name":  "apple",
							"price": 2.5,
						},
					},
					Child:  nil,
					Parent: nil,
				},
			},
			Parent: nil,
		},
		Order: 0,
	}

	for _, c := range expectedTree.Root.Child {
		c.Parent = expectedTree.Root
	}

	actualTree, err := core.LoadBPlusTreeFromJson(jsonData)
	if err != nil {
		t.Fatal(err)
	}

	if !expectedTree.CompareBPlusTrees(actualTree) {
		t.Errorf("Expected tree %v, but got tree %v", expectedTree, actualTree)
	}
}
