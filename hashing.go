package main

import (
	"fmt"
	"hash/crc32"
)

const NUM_NODES = 5

func display(db *DB) {
	fmt.Printf("current state:\n")
	for _, node := range db.nodes {
		fmt.Printf("Node id: %v, positions on ring: [%v, %v, %v, %v]\n", node.id, node.positions[0], node.positions[1], node.positions[2], node.positions[3])
	}
	fmt.Println()
}

func main() {
	db := NewDB()

	for i := 0; i < NUM_NODES; i++ {
		db.AddNode()
	}
	display(db)

	db.Put("Maria", 100)
	db.Put("John", 20)
	db.Put("Anna", 40)
	db.Put("Tim", 100)
	db.Put("Alex", 10)

	keys := [5]string{
		"Maria", "John", "Anna", "Tim", "Alex",
	}

	for _, k := range keys {
		fmt.Printf("Hash value of %v: %v\n", k, int(crc32.ChecksumIEEE([]byte(k)))%360)
	}
	fmt.Println()

	fmt.Println("Initial Locations:")
	for _, k := range keys {
		fmt.Println(k+":", db.GetNodeIdForKey(k))
	}
	fmt.Println()

	fmt.Println("Deleting Node 1\n")
	db.DeleteNode(db.nodes[1])
	display(db)

	fmt.Println("New Locations")
	for _, k := range keys {
		fmt.Println(k+":", db.GetNodeIdForKey(k))
	}

}
