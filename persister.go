package main

import "sync"

type Persister struct {
	currentTerm int
	votedFor    int
	log         []*LogEntry
	mux 			sync.RWMutex
}

func initPersister() (*Persister) {
	return &Persister {currentTerm: 0, votedFor: -1, log: []*LogEntry{}}
}


func (worker *Worker) saveTerm(){
	worker.mux.RLock() 
	worker.persister.mux.Lock()
	worker.persister.currentTerm = worker.term
	worker.persister.mux.Unlock()
	worker.mux.RUnlock()
}

func (worker *Worker) saveVotedFor(){
	worker.mux.RLock() 
	worker.persister.mux.Lock() 
	worker.persister.votedFor = worker.votedFor
	worker.persister.mux.Unlock()
	worker.mux.RUnlock()
}

func (worker *Worker) saveLog(entry *LogEntry) {
	worker.mux.RLock() 
	worker.persister.mux.Lock() 
	worker.persister.log = append(worker.persister.log, 
		&LogEntry{index:entry.index, term:entry.term, command:entry.command})
	worker.persister.mux.Unlock()
	worker.mux.RUnlock()
}