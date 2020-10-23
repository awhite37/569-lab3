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
	worker.persister.mux.Lock() 
	worker.persister.currentTerm = worker.term
	worker.persister.mux.Unlock()
}

func (worker *Worker) saveVotedFor(){
	worker.persister.mux.Lock() 
	worker.persister.votedFor = worker.votedFor
	worker.persister.mux.Unlock()
}

func (worker *Worker) saveLog(entry *LogEntry) {
	worker.persister.mux.Lock() 
	worker.persister.log = append(worker.persister.log, 
		&LogEntry{index:entry.index, term:entry.term, command:entry.command})
	worker.persister.mux.Unlock()
}