package main

import "math/rand"
import "time"
import "fmt"
import "sync"

//time to wait before counting votes
const Z = 50 * time.Millisecond

//time to wait before sending heartbeat
const X = 50 * time.Millisecond

type Worker struct {
	id        int
	applyCh   chan ApplyMsg
	entriesCh chan EntryMsg
	peers     []*Worker

	term        int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	isLeader       bool
	isCandidate    bool
	votedFor       int
	voteinput      chan VoteRequest
	votes          chan Response
	leaderCommand	chan interface{}
	appendResponse chan Response
	numSuccess     chan int
	timeout        chan int
	mux            sync.RWMutex

	persistor Persistor

	log []*LogEntry
}

type VoteRequest struct {
	from      *Worker
	term      int
	lastTerm  int
	lastIndex int
}

type Response struct {
	term    int
	granted bool
}

type EntryMsg struct {
	term         int
	leaderID     int
	prevIndex    int
	prevTerm     int
	entries      []LogEntry
	leaderCommit int
}

type ApplyMsg struct {
}

type LogEntry struct {
	command interface{}
	term    int
	index   int
}

type Persistor struct {
}

func (worker *Worker) run() {
	go worker.HB()
	go worker.respondToVotes()
	go worker.electionTimeout()
	go worker.handleMsg()
	go worker.applyEntries()
	for {
		//let worker run
	}
}

func (worker *Worker) requestVotes(term int) {
	votes := 1
	for _, peer := range worker.peers {
		peer.voteinput <- VoteRequest{
			from:      worker,
			term:      term,
			lastIndex: worker.commitIndex,
		}
	}
	time.Sleep(Z)
	//quit if accepted other leader
	worker.mux.RLock()
	isCandidate := worker.isCandidate
	newTerm := worker.term
	id := worker.id
	worker.mux.RUnlock()
	if !isCandidate || newTerm != term {
		fmt.Printf("node %d no longer candidate for term %d\n", id, term)
		return
	}
	for len(worker.votes) > 0 {
		vote := <-worker.votes
		if vote.term > term {
			worker.revert(vote.term)
			return
		}
		if vote.term == term && vote.granted {
			votes += 1
		}
	}
	//check for majority
	worker.mux.RLock()
	term2 := worker.term
	worker.mux.RUnlock()
	fmt.Printf("node %d got %d votes for term %d\n", id, votes, term2)
	if votes > (len(worker.peers)+1)/2 {
		fmt.Printf("node %d is leader for term %d\n", id, term2)
		worker.mux.Lock()
		worker.isLeader = true
		worker.isCandidate = false
		worker.term = term
		worker.mux.Unlock()
		worker.initLeader()
		//reset election timeout
		worker.mux.RLock()
		term2 := worker.term
		worker.mux.RUnlock()
		for _, peer := range worker.peers {
			peer.entriesCh <- EntryMsg{entries: nil, term: term2, leaderID: id}
		}
		worker.leaderAppend()
	}
}

func (worker *Worker) electionTimeout() {
	rand.Seed(time.Now().UnixNano())
	for {
		worker.mux.RLock()
		isLeader := worker.isLeader
		worker.mux.RUnlock()
		if !isLeader {
			//wait for leader, then become candidate
			select {
			case <-worker.timeout:
				break
			//random timeout between 150-300ms
			case <-time.After(time.Duration(rand.Intn(150)+151) * time.Millisecond):
				worker.mux.RLock()
				id := worker.id
				term := worker.term
				worker.mux.RUnlock()
				worker.mux.Lock()
				worker.votedFor = id
				worker.isCandidate = true
				worker.term += 1
				worker.mux.Unlock()
				fmt.Printf("node %d becoming candidate for term %d\n", id, term+1)
				worker.requestVotes(term + 1)
				break
			}
		}
	}
}

func (worker *Worker) handleMsg() {
	for {
		msg := <-worker.entriesCh
		worker.mux.RLock()
		term := worker.term
		worker.mux.RUnlock()
		if msg.term >= term {
			worker.mux.Lock()
			worker.isCandidate = false
			worker.isLeader = false
			worker.term = msg.term
			worker.mux.Unlock()
			//message from leader, reset election timeout
			worker.timeout <- 1
			//handle message
			if msg.entries != nil {
				if msg.prevIndex != -1 {
					worker.mux.RLock()
					logTerm := worker.log[msg.prevIndex].term
					worker.mux.RUnlock()
					if logTerm != msg.prevTerm {
						worker.appendResponse <- Response{term: term, granted: false}
						continue
					}
				}	
				worker.mux.RLock()
				commitIndex := worker.commitIndex
				worker.mux.RUnlock()
				worker.mux.Lock()
				worker.log = append(worker.log[0:msg.prevIndex+1], &msg.entries[0])
				worker.mux.Unlock()
				if msg.leaderCommit > commitIndex {
					worker.mux.Lock()
					worker.commitIndex = Min(msg.leaderCommit, len(worker.log)-1)
					worker.mux.Unlock()
				}
				worker.appendResponse <- Response{term:term, granted: true}
			}
		} else if msg.entries != nil {
			worker.appendResponse <- Response{term: term, granted: false}
		}
	}
}

func (worker *Worker) respondToVotes() {
	highestVoted := 0
	for {
		vote := <-worker.voteinput
		worker.mux.RLock()
		term := worker.term
		isCandidate := worker.isCandidate
		worker.mux.RUnlock()
		if vote.term > term {
			worker.revert(vote.term)
		}
		worker.mux.RLock()
		isCandidate = worker.isCandidate
		isLeader := worker.isLeader
		worker.mux.RUnlock()
		if !isLeader && !isCandidate {
			if vote.term > highestVoted {
				highestVoted = vote.term
				worker.mux.Lock()
				worker.votedFor = -1
				worker.mux.Unlock()
			}
			worker.mux.RLock()
			votedFor := worker.votedFor
			term := worker.term
			id := worker.id
			commitIndex := worker.commitIndex
			worker.mux.RUnlock()
			if vote.term > term {
				worker.mux.Lock()
				worker.term = vote.term
				worker.mux.Unlock()
			}
			fmt.Printf("node %d term %d got vote request from node %d on term %d, curr voted for: %d\n", id, term, vote.from.id, vote.term, votedFor)
			if vote.term >= term &&
				(votedFor == -1 || votedFor == vote.from.id) && vote.lastIndex >= commitIndex {
				//grant vote
				worker.mux.Lock()
				worker.term = vote.term
				worker.votedFor = vote.from.id
				worker.mux.Unlock()
				fmt.Printf("node %d voting for node %d on term %d\n", id, vote.from.id, vote.term)
				(vote.from).votes <- Response{term: vote.term, granted: true}
				//restart election timer
				worker.timeout <- 1

			} else {
				(vote.from).votes <- Response{term: term, granted: false}
			}

		}

	}
}

func (worker *Worker) HB() {
	worker.mux.RLock()
	id := worker.id
	worker.mux.RUnlock()
	if id < 10 {
		for {
			time.Sleep(X)
			worker.mux.RLock()
			isLeader := worker.isLeader
			worker.mux.RUnlock()
			if isLeader {
				worker.mux.RLock()
				id := worker.id
				term := worker.term
				worker.mux.RUnlock()
				for _, peer := range worker.peers {
					peer.entriesCh <- EntryMsg{entries: nil, term: term, leaderID: id}
				}
			}
		}
	}
}

func (worker *Worker) revert(term int) {
	worker.mux.Lock()
	worker.isCandidate = false
	worker.term = term
	worker.mux.Unlock()
}

func (worker *Worker) applyEntries() {
	for {
		time.Sleep(1*time.Second)
		worker.mux.RLock()
		commitIndex := worker.commitIndex
		lastApplied := worker.lastApplied
		worker.mux.RUnlock()
		if commitIndex > lastApplied {
			worker.mux.Lock()
			worker.lastApplied += 1
			//apply log[last applied] to state machine
			worker.mux.Unlock()
		}
	}
	
}

func (worker *Worker) initLeader() {
	nextIndex := make([]int, len(worker.peers))
	matchIndex := make([]int, len(worker.peers))
	worker.mux.RLock()
	index := len(worker.log) - 1
	worker.mux.RUnlock()
	for i, _ := range worker.peers {
		nextIndex[i] = index + 1
		matchIndex[i] = 0
	}
	worker.mux.Lock()
	worker.nextIndex = nextIndex
	worker.matchIndex = matchIndex
	worker.mux.Unlock()
}

func (worker *Worker) leaderAppend() {
	for {
		worker.mux.RLock()
		isLeader := worker.isLeader
		worker.mux.RUnlock()
		
		if !isLeader {
			return
		}
		command := <- worker.leaderCommand
		worker.mux.RLock()
		currTerm := worker.term
		index := len(worker.log) - 1
		worker.mux.RUnlock()
		entry := LogEntry{command: command, term: currTerm, index: index}
		worker.mux.Lock()
		worker.log = append(worker.log, &entry)
		worker.mux.Unlock()

		numSuccess := 0
		for i, peer := range worker.peers {
			worker.mux.RLock()
			next := worker.nextIndex[i]
			worker.mux.RUnlock()
			if (index + 1) >= next {
				go worker.sendAppends(peer, i, index, &entry)
			}
		}
		worker.mux.RLock()
		term := (worker.log[worker.commitIndex]).term
		worker.mux.RUnlock()
		for {
			worker.mux.RLock()
			isLeader := worker.isLeader
			worker.mux.RUnlock()
			if !isLeader {
				return
			}
			numSuccess += <-worker.numSuccess
			if numSuccess > len(worker.peers)/2 && term == currTerm {
				worker.mux.Lock()
				worker.commitIndex += 1
				worker.mux.Unlock()
				for _, peer := range worker.peers { 
					peer.applyCh <- ApplyMsg{}
				}
				break
			}
		}
	}
}

func (worker *Worker) sendAppends(peer *Worker, i int, index int, entry *LogEntry) {
	prevTerm := -1
	worker.mux.RLock()
	next := worker.nextIndex[i]
	id := peer.id
	currTerm := worker.term
	leaderCommit := worker.commitIndex
	worker.mux.RUnlock()
	entries := []LogEntry{}
	entries = append(entries, *entry)
	for (index + 1) >= next {
		if index != -1 {
			worker.mux.RLock()
			prevTerm = worker.log[index].term
			worker.mux.RUnlock()
		}
		apply := EntryMsg{
			term:         currTerm,
			leaderID:     id,
			prevIndex:    index,
			prevTerm:     prevTerm,
			entries:      entries,
			leaderCommit: leaderCommit,
		}
		peer.entriesCh <- apply
		response := <-peer.appendResponse
		if response.granted {
			worker.mux.Lock()
			worker.nextIndex[i] += 1
			worker.matchIndex[i] += 1
			worker.mux.Unlock()
			worker.numSuccess <- 1
			return
		} else if response.term > currTerm {
			worker.mux.Lock()
			worker.isCandidate = false
			worker.isLeader = false
			worker.term = response.term
			worker.mux.Unlock()
			return
		} else {
			worker.mux.Lock()
			worker.nextIndex[i] -= 1
			worker.mux.Unlock()
			next = -1
		}
	}
}

func Min(x, y int) int {
 if x < y {
   return x
 }
 return y
}

func Make(peers []*Worker, me int, persistor Persistor, applyCh chan ApplyMsg) (worker *Worker) {
	workers := []*Worker{}
	for _, peer := range peers {
		workers = append(workers, peer)
	}
	new := Worker{
		id:             me,
		applyCh:        applyCh,
		entriesCh:      make(chan EntryMsg),
		peers:          workers,
		term:           0,
		commitIndex:    0,
		lastApplied:    0,
		isLeader:       false,
		isCandidate:    false,
		votedFor:       -1,
		voteinput:      make(chan VoteRequest, 10),
		votes:          make(chan Response, 10),
		appendResponse: make(chan Response, 10),
		leaderCommand:  make(chan interface{}, 10),
		numSuccess:     make(chan int, 10),
		timeout:        make(chan int, 10),
		persistor:      persistor,
		log:            []*LogEntry{},
	}
	for _, peer := range peers {
		peer.peers = append(peer.peers, &new)
	}
	return &new
}

func (worker *Worker) Start(command interface{}) (int, int, bool) {
	worker.mux.RLock()
	id := worker.id
	term := worker.term
	isLeader := worker.isLeader
	worker.mux.RUnlock()
	worker.leaderCommand <- command
	for _, peer := range worker.peers {
		peer.leaderCommand <- command
	}
	return id, term, isLeader
}

func (worker *Worker) GetState() (int, bool) {
	worker.mux.RLock()
	term := worker.term
	isLeader := worker.isLeader
	worker.mux.RUnlock()
	return term, isLeader
}
