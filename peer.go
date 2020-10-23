package main

import "math/rand"
import "time"
import "fmt"
import "sync"

//time to wait before counting votes
const Z = 50 * time.Millisecond

//time to wait before sending heartbeat
const X = 50 * time.Millisecond

//time to wait before checking if commands can be applied
const Y = 50 * time.Millisecond

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
	appendResponse chan Response
	numSuccess     chan int
	timeout        chan int
	mux            sync.RWMutex

	persister *Persister

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
	lastTerm := -1
	worker.mux.RLock()
	index := worker.commitIndex
	worker.mux.RUnlock()
	if len(worker.log) < index {
		worker.mux.RLock()
		lastTerm = worker.log[index].term
		worker.mux.RUnlock()
	}
	for _, peer := range worker.peers {
		peer.voteinput <- VoteRequest{
			from:      worker,
			term:      term,
			lastIndex: index,
			lastTerm:  lastTerm,
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
		worker.initLeader()
		worker.mux.Lock()
		worker.isLeader = true
		worker.isCandidate = false
		worker.term = term
		worker.mux.Unlock()
		worker.saveTerm()
		//reset election timeout
		worker.mux.RLock()
		term2 := worker.term
		worker.mux.RUnlock()
		for _, peer := range worker.peers {
			peer.entriesCh <- EntryMsg{entries: nil, term: term2, leaderID: id}
		}
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
				worker.saveTerm()
				worker.saveVotedFor()
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
			worker.saveTerm()
			//message from leader, reset election timeout
			worker.timeout <- 1
			//handle message
			worker.mux.RLock()
			commitIndex := worker.commitIndex
			worker.mux.RUnlock()
			if msg.leaderCommit > commitIndex {
				worker.mux.Lock()
				worker.commitIndex = Min(msg.leaderCommit, len(worker.log)-1)
				worker.mux.Unlock()
			}
			if msg.entries != nil {
				if msg.prevIndex > -1 && len(worker.log) <= msg.prevIndex {
						worker.appendResponse <- Response{term: term, granted: false}
						continue
					}
				if msg.prevIndex != -1 {
					worker.mux.RLock()
					logTerm := worker.log[msg.prevIndex].term
					worker.mux.RUnlock()
					if logTerm != msg.prevTerm {
						worker.appendResponse <- Response{term: term, granted: false}
						continue
					}
					
				}
				worker.restoreLog(msg.leaderID, msg.leaderCommit, msg.prevIndex)
				worker.mux.Lock()
				worker.log = append(worker.log, &msg.entries[0])
				worker.mux.Unlock()
				worker.saveLog(&msg.entries[0])
				
				worker.appendResponse <- Response{term: term, granted: true}
			}
		} else if msg.entries != nil {
			worker.appendResponse <- Response{term: term, granted: false}
		}
	}
}

func (worker *Worker) restoreLog(leaderID int, leaderCommit int, prevIndex int) {
	var leader *Worker
	for _, peer := range(worker.peers) {
		if peer.id == leaderID {
			leader = peer
			break
		}
	}
	worker.mux.Lock()
	worker.log = worker.log[:prevIndex+1]
	worker.persister.log = worker.persister.log[:prevIndex+1]
	worker.mux.Unlock()
	for i := prevIndex+1; i <= leaderCommit; i++ {
		leader.mux.RLock()
		entry := leader.log[i]
		leader.mux.RUnlock()  
		entryCpy := LogEntry{index:entry.index, term:entry.term, command:entry.command}
		worker.mux.Lock()
		worker.log = append(worker.log, &entryCpy)
		worker.mux.Unlock()
		worker.saveLog(&entryCpy)
	}
}

func (worker *Worker) respondToVotes() {
	highestVoted := 0
	for {
		vote := <-worker.voteinput
		worker.mux.RLock()
		term := worker.term
		worker.mux.RUnlock()
		if vote.term > term {
			worker.revert(vote.term)
		}
		worker.mux.RLock()
		isCandidate := worker.isCandidate
		isLeader := worker.isLeader
		worker.mux.RUnlock()
		if !isLeader && !isCandidate {
			if vote.term > highestVoted {
				highestVoted = vote.term
				worker.mux.Lock()
				worker.votedFor = -1
				worker.mux.Unlock()
				worker.saveVotedFor()
			}
			worker.mux.RLock()
			votedFor := worker.votedFor
			term := worker.term
			id := worker.id
			commitIndex := worker.commitIndex
			lastTerm := -1
			if len(worker.log) < commitIndex {
				worker.mux.RLock()
				lastTerm = worker.log[commitIndex].term
				worker.mux.RUnlock()
			}
			worker.mux.RUnlock()
			if vote.term > term {
				worker.mux.Lock()
				worker.term = vote.term
				worker.mux.Unlock()
				worker.saveTerm()
			}
			if vote.term >= term &&
				(votedFor == -1 || votedFor == vote.from.id) &&
				vote.lastIndex >= commitIndex && vote.lastTerm >= lastTerm {
				//restart election timer
				worker.timeout <- 1
				//grant vote
				worker.mux.Lock()
				worker.term = vote.term
				worker.votedFor = vote.from.id
				worker.mux.Unlock()
				worker.saveTerm()
				worker.saveVotedFor()
				fmt.Printf("node %d voting for node %d on term %d\n", id, vote.from.id, vote.term)
				(vote.from).votes <- Response{term: vote.term, granted: true}
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
	//use this to configure which nodes can send HB for testing
	if id > -1 {
		for {
			time.Sleep(X)
			worker.mux.RLock()
			isLeader := worker.isLeader
			worker.mux.RUnlock()
			if isLeader {
				worker.mux.RLock()
				id := worker.id
				term := worker.term
				leaderCommit := worker.commitIndex
				worker.mux.RUnlock()
				for _, peer := range worker.peers {
					peer.entriesCh <- EntryMsg{entries: nil, term: term, leaderID: id, leaderCommit: leaderCommit}
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
	worker.saveTerm()
}

func (worker *Worker) applyEntries() {
	for {
		time.Sleep(Y)
		worker.mux.RLock()
		commitIndex := worker.commitIndex
		lastApplied := worker.lastApplied
		worker.mux.RUnlock()
		if commitIndex > lastApplied {
			worker.mux.Lock()
			worker.lastApplied += 1
			worker.applyCh <- ApplyMsg{}
			worker.mux.Unlock()
		}
	}

}

func (worker *Worker) initLeader() {
	nextIndex := make([]int, len(worker.peers))
	matchIndex := make([]int, len(worker.peers))
	worker.mux.RLock()
	index := worker.commitIndex
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

func (worker *Worker) leaderAppend(command interface{}) {
	worker.mux.RLock()
	currTerm := worker.term
	prevIndex := worker.commitIndex+1
	id := worker.id
	worker.mux.RUnlock()
	entry := LogEntry{command: command, term: currTerm, index: prevIndex+1}
	worker.mux.Lock()
	worker.log = worker.log[:prevIndex]
	worker.persister.log = worker.persister.log[:prevIndex]
	worker.log = append(worker.log, &entry)
	worker.mux.Unlock()
	worker.saveLog(&entry)

	numSuccess := 0
	for i, peer := range worker.peers {
		worker.mux.RLock()
		nextIndex := worker.nextIndex[i]
		worker.mux.RUnlock()
		if (prevIndex) >= nextIndex {
			go worker.sendAppends(peer, i, currTerm, &entry, prevIndex-1)
		}
	}
	worker.mux.RLock()
	term := (worker.log[len(worker.log)-1]).term
	isLeader := worker.isLeader
	worker.mux.RUnlock()
	for isLeader && term == currTerm{
		success := <-worker.numSuccess
		if success == currTerm {
			numSuccess += 1
		}
		worker.mux.RLock()
		isLeader = worker.isLeader
		worker.mux.RUnlock()
		if numSuccess > len(worker.peers)/2 {
			if isLeader {
				worker.mux.Lock()
				worker.commitIndex += 1
				leaderCommit := worker.commitIndex
				worker.mux.Unlock()
				for _, peer := range worker.peers {
					peer.entriesCh <- EntryMsg{entries: nil, term: term, leaderID: id, leaderCommit: leaderCommit}
				}
			}
			break
		}
	}
}

func (worker *Worker) sendAppends(peer *Worker, i int, leaderTerm int, entry *LogEntry, leaderCommit int) {
	prevTerm := -1
	worker.mux.RLock()
	nextIndex := worker.nextIndex[i] - 1
	id := worker.id
	worker.mux.RUnlock()
	entries := []LogEntry{}
	entries = append(entries, *entry)
	for {
		if nextIndex != -1 {
			worker.mux.RLock()
			prevTerm = worker.log[nextIndex].term
			worker.mux.RUnlock()
		} else {
			prevTerm = -1
		}
		apply := EntryMsg{
			term:         leaderTerm,
			leaderID:     id,
			prevIndex:    nextIndex,
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
			worker.numSuccess <- leaderTerm
			return
		} else if response.term > leaderTerm {
			worker.mux.Lock()
			worker.isCandidate = false
			worker.isLeader = false
			worker.term = response.term
			worker.mux.Unlock()
			worker.saveTerm()
			return
		} else {
			worker.mux.Lock()
			worker.nextIndex[i] -= 1
			worker.mux.Unlock()
			nextIndex -= 1
		}
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Make(peers []*Worker, me int, persister *Persister, applyCh chan ApplyMsg) (worker *Worker) {
	workers := []*Worker{}
	for _, peer := range peers {
		workers = append(workers, peer)
	}
	new := Worker{
		id:             me,
		applyCh:        applyCh,
		entriesCh:      make(chan EntryMsg),
		peers:          workers,
		term:           persister.currentTerm,
		commitIndex:    -1,
		lastApplied:    -1,
		isLeader:       false,
		isCandidate:    false,
		votedFor:       persister.votedFor,
		voteinput:      make(chan VoteRequest, 10),
		votes:          make(chan Response, 10),
		appendResponse: make(chan Response, 10),
		numSuccess:     make(chan int, 10),
		timeout:        make(chan int, 10),
		persister:      persister,
		log:            persister.log,
	}

	for _, peer := range peers {
		peer.peers = append(peer.peers, &new)
	}
	return &new
}

func (worker *Worker) Start(command interface{}) (int, int, bool) {
	worker.mux.RLock()
	id := worker.id
	isLeader := worker.isLeader
	term := worker.term
	worker.mux.RUnlock()
	var leader *Worker
	leader = nil
	for leader == nil {
		worker.mux.RLock()
		isLeader = worker.isLeader
		worker.mux.RUnlock()
		if isLeader {
			leader = worker
			break
		}
		for _, peer := range worker.peers {
			peer.mux.RLock()
			isLeaderPeer := peer.isLeader
			peer.mux.RUnlock()
			if isLeaderPeer {
				leader = peer
				break
			}
		}
	}
	go leader.leaderAppend(command)
	return id, term, isLeader
}

func (worker *Worker) GetState() (int, bool) {
	worker.mux.RLock()
	term := worker.term
	isLeader := worker.isLeader
	worker.mux.RUnlock()
	return term, isLeader
}

func (worker *Worker) printLog() {
	worker.mux.RLock()
	for _, entry := range(worker.log) {
		fmt.Printf("index %d: term: %d KeyVal: (%s,%d) \n", entry.index, entry.term, (entry.command).(KeyVal).Key, (entry.command).(KeyVal).Val)
	}
	worker.mux.RUnlock()
}
