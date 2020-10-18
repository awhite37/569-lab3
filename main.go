package main

const numWorkers = 5

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
	for {}
}