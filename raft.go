package main

import "fmt"

const numWorkers = 5


type Command interface {
}

type keyval struct {
	Key	string
	Val   int
}




func main() {
	workers := []*Worker{}
	for i := 0; i < numWorkers; i++ {
		persistor := Persistor{}
		applyCh := make(chan ApplyMsg,10)
		new := Make(workers, i, persistor, applyCh)
		workers = append(workers, new)
	}
	for _, worker := range(workers) {
		go worker.run()
	}
	command1 := keyval{Key: "x", Val: 10}
	command2 := keyval{Key: "y", Val: 20}
	command3 := keyval{Key: "z", Val: 30}
	fmt.Printf("issuing command 1\n")
	workers[0].Start(command1)
	<- workers[0].applyCh
	fmt.Printf("command 1 successfully committed\n")
	fmt.Printf("issuing command 2\n")
	workers[0].Start(command2)
	<- workers[0].applyCh
	fmt.Printf("command 2 successfully committed\n")
	fmt.Printf("issuing command 3\n")
	workers[0].Start(command3)
	<- workers[0].applyCh
	fmt.Printf("command 3 successfully committed\n")
	for i, worker := range(workers) {
		fmt.Printf("\nWorker %d's log\n", i)
		for _, entry := range(worker.log) {
			fmt.Printf("index %d: term: %d KeyVal: (%s,%d) \n", entry.index, entry.term, (entry.command).(keyval).Key, (entry.command).(keyval).Val)
		}
	}
	fmt.Printf("\n")
}