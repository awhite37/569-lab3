package main

import (
	"math/rand"
	"hash/crc32"
)

type Node struct {
	id        int
	positions [4]int
	data      [4]map[string]int
}

type VirtualNode struct {
	position    int
	indexInNode int
	node        *Node
}

type DB struct {
	nodes  []*Node
	ring   []VirtualNode
	nextId int
}

func NewDB() *DB {
	return &DB{
		nodes:  make([]*Node, 0, 20),
		ring:   make([]VirtualNode, 0, 80),
		nextId: 0,
	}
}

func getPos() [4]int {
	//return 4 random positions on the ring
	// doesn't have to be 360, but might as well be
	return [4]int{
		rand.Intn(360),
		rand.Intn(360),
		rand.Intn(360),
		rand.Intn(360),
	}
}

func (vn VirtualNode) data() *map[string]int {
	return &vn.node.data[vn.indexInNode]
}

func (db *DB) AddNode() {
	node := Node{
		id:        db.nextId,
		positions: getPos(),
		data: [4]map[string]int{
			make(map[string]int, 20),
			make(map[string]int, 20),
			make(map[string]int, 20),
			make(map[string]int, 20),
		},
	}
	db.nextId += 1

	db.nodes = append(db.nodes, &node)

	for j := 0; j < 4; j++ {
		vnode := VirtualNode{
			position:    node.positions[j],
			node:        &node,
			indexInNode: j,
		}
		if len(db.ring) == 0 {
			db.ring = append(db.ring, vnode)
		} else {
			for i, vn := range db.ring {
				if vn.position > node.positions[j] {
					db.ring = append(db.ring, VirtualNode{})
					copy(db.ring[i+1:], db.ring[i:])
					db.ring[i] = vnode
					break
				}
				if i == len(db.ring)-1 {
					db.ring = append(db.ring, vnode)
				}
			}
		}
	}
}

func (db *DB) DeleteNode(node *Node) {
	for i, n := range db.nodes {
		if node == n {
			db.nodes = append(db.nodes[:i], db.nodes[i+1:]...)
			break
		}
	}

	for i, vn := range db.ring {
		if vn.node == node {
			db.ring = append(db.ring[:i], db.ring[i+1:]...)

			toTransfer := vn.data()
			sendTo := db.ring[i].data()

			for key, value := range *toTransfer {
				(*sendTo)[key] = value
			}
		}
	}
}

func (db *DB) getVNodeForKey(key string) VirtualNode {
	keyHash := int(crc32.ChecksumIEEE([]byte(key)))
	keyHash %= 360
	target := db.ring[len(db.ring)-1]
	for i, _ := range db.ring {
		if db.ring[len(db.ring)-i-1].position < keyHash {
			break
		} else {
			target = db.ring[len(db.ring)-i-1]
		}
	}

	return target
}

func (db *DB) getDataForKey(key string) *map[string]int {
	return db.getVNodeForKey(key).data()
}

func (db *DB) GetNodeIdForKey(key string) int {
	return db.getVNodeForKey(key).node.id
}

func (db *DB) Get(key string) int {
	return (*db.getDataForKey(key))[key]
}

func (db *DB) Put(key string, value int) {
	(*db.getDataForKey(key))[key] = value
}
