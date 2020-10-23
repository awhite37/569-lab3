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
	persisters := []*Persister{}
	for i := 0; i < numWorkers; i++ {
		persister := initPersister()
		applyCh := make(chan ApplyMsg, 10)
		new := Make(workers, i, persister, applyCh)
		workers = append(workers, new)
		persisters = append(persisters, persister)
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
	fmt.Printf("\ndisplaying saved Persister states:\n")
	for i, p := range(persisters) {
		p.mux.RLock()
		fmt.Printf("\nWorker %d's currentTerm: %d\n", i, p.currentTerm)
		fmt.Printf("Worker %d's votedFor: %d\n", i, p.votedFor)
		fmt.Printf("Worker %d's perister log\n", i)
		p.mux.RUnlock()
		displayPersisterLog(p)	

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

func displayPersisterLog(persister *Persister) {
	persister.mux.RLock()
	for _, entry := range(persister.log) {
		fmt.Printf("index %d: term: %d KeyVal: (%s,%d) \n", entry.index, entry.term, (entry.command).(KeyVal).Key, (entry.command).(KeyVal).Val)
	}
	persister.mux.RUnlock()
}


