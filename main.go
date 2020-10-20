package main

import "fmt"

const numWorkers = 5


type Commands interface {
	command()
}

type keyval struct {
	Key	int
	Val   string
}

func (kv keyval) command() {
	//empty command for testing
}


func main() {
	workers := []*Worker{}
	for i := 0; i < numWorkers; i++ {
		persistor := Persistor{}
		applyCh := make(chan ApplyMsg)
		new := Make(workers, i, persistor, applyCh)
		workers = append(workers, new)
	}
	for _, worker := range(workers) {
		go worker.run()
	}
	command := keyval{}
	fmt.Printf("issuing command\n")
	workers[0].Start(command)
	<- workers[0].applyCh
	fmt.Printf("command successfully committed\n")
}