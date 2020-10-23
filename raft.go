package main

import "fmt"
import "time"

const numWorkers = 5
const N = 3000 * time.Millisecond

type KeyVal struct {
	Key	string
	Val   int
}




func main() {
	workers := []*Worker{}
	for i := 0; i < numWorkers; i++ {
		persistor := Persistor{}
		applyCh := make(chan ApplyMsg, 10)
		new := Make(workers, i, persistor, applyCh)
		workers = append(workers, new)
	}
	
	for _, worker := range(workers) {
		go worker.run()
	}
	command1 := KeyVal{Key: "x", Val: 10}
	command2 := KeyVal{Key: "y", Val: 20}
	command3 := KeyVal{Key: "z", Val: 30}
	fmt.Printf("issuing command 1\n")
	applyCommand(command1, workers[0])
	fmt.Printf("command 1 successfully committed\n")
	fmt.Printf("issuing command 2\n")
	applyCommand(command2, workers[0])
	fmt.Printf("command 2 successfully committed\n")
	fmt.Printf("issuing command 3\n")
	applyCommand(command3, workers[0])
	fmt.Printf("command 3 successfully committed\n")

	for i, worker := range(workers) {
		fmt.Printf("\nWorker %d's log\n", i)
		worker.printLog()
	}
	fmt.Printf("\n")
}

func applyCommand(command interface{}, worker *Worker) {
	worker.Start(command)
	for {
	   select {
	   case <-worker.applyCh:
	   	return
	   case <-time.After(N):
	   	fmt.Printf("leader crashed before commit, re-issuing command \n")
	   	worker.Start(command)
	   	continue
		}
	}
}


