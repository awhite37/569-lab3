package main

type Persistor struct {
	currentTerm int
	votedFor    int
	log         []*LogEntry
}
