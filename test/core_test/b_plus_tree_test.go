package core_test

import (
	"ne_database/utils"
	"reflect"
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
	for i, child := range root.Child {
		if utils.ToJSON(child.Keys) != utils.ToJSON(root.Keys[i:i+1]) {
			t.Errorf("JsonToBPlusTree failed: child.keys=%v, root.keys=%v", child.Keys, root.Keys[i:i+1])
		}
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
			  "child": null,
			  "parent": {}
			}
		  ],
		  "parent": {}
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

	actualNode, err := core.JsonToBPlusTree(jsonData)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedNode, actualNode) {
		t.Errorf("Expected node %v, but got node %v", expectedNode, actualNode)
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
			  "child": null,
			  "parent": {}
			}
		  ],
		  "parent": {}
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

	actualTree, err := core.LoadBPlusTreeFromJson(jsonData)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedTree, actualTree) {
		t.Errorf("Expected tree %v, but got tree %v", expectedTree, actualTree)
	}
}
