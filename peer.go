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
	id 		   int
	applyCh     chan ApplyMsg
	peers       []*Worker
	term		   int
	commitIndex	int

	isLeader    bool
	isCandidate bool
	votedFor		int
	voteinput	chan VoteRequest
	votes       chan VoteResponse
	timeout     chan int
	mux 			sync.RWMutex

	persistor   Persistor

	log			[]*LogEntry
	
}

type VoteRequest struct {
	from   	 *Worker
	term      int
	lastTerm  int
	lastIndex int
}

type VoteResponse struct {
	term 		int
	granted  bool
}

type ApplyMsg struct {
	term 		    int
	leaderID     int
	prevIndex    int
	prevTerm     int
	entries      []*LogEntry
	leaderCommit int
}

type LogEntry struct {
	command interface{}
	index	  int
	term    int
}

type Persistor struct {

}
 

func (worker *Worker) run() {
	go worker.HB()
	go worker.respondToVotes()
	go worker.electionTimeout()
	go worker.handleMsg()
	for {
		//let worker run
	}
}



func (worker *Worker) requestVotes(term int) {
	votes := 1
	for _, peer := range(worker.peers) {
		peer.voteinput <- VoteRequest{
			from: worker, 
			term: term, 
			lastIndex: worker.commitIndex,
		}
	}
	time.Sleep(Z)
	//quit if accepted other leader
	worker.mux.RLock()
	isCandidate := worker.isCandidate
	worker.mux.RUnlock()
	if !isCandidate {
		worker.mux.RLock()
		id := worker.id
		term2 := worker.term
		worker.mux.RUnlock()
		fmt.Printf("node %d no longer candidate for term %d\n", id, term2)
		return
	}
	for len(worker.votes) > 0 {
		vote := <- worker.votes
		if vote.term > term {
			worker.mux.Lock()
			worker.isCandidate = false
			worker.term = vote.term
			worker.mux.Unlock()
			worker.mux.RLock()
			id := worker.id
			term2 := worker.term
			worker.mux.RUnlock()
			fmt.Printf("node %d reverting to follower on term %d\n", id, term2)
			return
		}
		if vote.term == term && vote.granted {
			votes += 1
		}
	}
	//check for majority
	worker.mux.RLock()
	id := worker.id
	term2 := worker.term
	worker.mux.RUnlock()
	fmt.Printf("node %d got %d votes for term %d\n", id, votes, term2)
	if (votes > (len(worker.peers)+1)/2) {
		fmt.Printf("node %d is leader for term %d\n", id, term2)
		worker.mux.Lock()
		worker.isLeader = true
		worker.isCandidate = false
		worker.term = term
		worker.mux.Unlock()
		//reset election timeout
		worker.mux.RLock()
		id := worker.id
		term2 := worker.term
		worker.mux.RUnlock()
		for _, peer := range(worker.peers) {
			peer.applyCh <- ApplyMsg {entries: nil, term: term2, leaderID: id}
		}
	}
}


func (worker *Worker) electionTimeout() {
	rand.Seed(time.Now().UnixNano())
	for {
		worker.mux.RLock()
		isLeader := worker.isLeader
		worker.mux.RUnlock()
		if (!isLeader) {
			//wait for leader, then become candidate
			select {
			case <- worker.timeout:
				break
			//random timeout between 150-300ms
			case <- time.After(time.Duration(rand.Intn(150) + 151) * time.Millisecond) : 
				worker.mux.Lock()
				worker.votedFor = worker.id
				worker.isCandidate = true
				worker.term += 1
				worker.mux.Unlock()
				worker.mux.RLock()
				id := worker.id
				term := worker.term
				worker.mux.RUnlock()
				fmt.Printf("node %d becoming candidate for term %d\n", id, term)
				worker.requestVotes(term)
				break
			}
		}
	}
}

func (worker *Worker) handleMsg() {
	for {
		msg := <- worker.applyCh
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
		}
	}
}

func (worker *Worker) respondToVotes() {
	highestVoted := 0
	for {
		worker.mux.RLock()
		isCandidate := worker.isCandidate
		isLeader := worker.isLeader
		worker.mux.RUnlock()
		if (!isLeader && !isCandidate) {
			vote := <- worker.voteinput
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
			fmt.Printf("node %d term %d got vote request from node %d on term %d, curr voted for: %d\n", id, term, vote.from.id, vote.term, worker.votedFor)
			if vote.term > term && 
				(votedFor == -1 || votedFor == vote.from.id) && vote.lastIndex >= commitIndex {
				//grant vote
				worker.mux.Lock()
				worker.term = vote.term
				worker.votedFor = vote.from.id
				worker.mux.Unlock()
				(vote.from).votes <- VoteResponse{term: vote.term, granted: true}
			
				//restart election timer
				worker.timeout <- 1
				worker.mux.RLock()
				id := worker.id
				worker.mux.RUnlock()
				fmt.Printf("node %d voting for node %d on term %d\n", id, vote.from.id, vote.term)
			} else {
				(vote.from).votes <- VoteResponse{term: term, granted: false}
			}

		} 
		
	}
}

func (worker *Worker) HB() {
	for{
		time.Sleep(X)
		worker.mux.RLock()
		isLeader := worker.isLeader
		worker.mux.RUnlock()
		if isLeader {
			worker.mux.RLock()
			id := worker.id
			term := worker.term
			worker.mux.RUnlock()
			fmt.Printf("curr leader: %d, term: %d\n", id, term)
			for _, peer := range(worker.peers) {
				peer.applyCh <- ApplyMsg {entries: nil, term: term, leaderID: id}
			}
		}
	}
}

func Make(peers []*Worker, me int, persistor Persistor, applyCh chan ApplyMsg) (worker *Worker) {
	workers := []*Worker{}
	for _, peer := range(peers) {
		workers = append(workers, peer)
	}
	new := Worker{
		id: 		   	me,
		applyCh:    	applyCh,
		peers:      	workers,
		term:		   	0,
		commitIndex:	0,
		isLeader:    	false,
		isCandidate:	false,
		votedFor:		-1,
		voteinput:		make(chan VoteRequest, 10),
		votes:       	make(chan VoteResponse, 10),
		timeout:        make(chan int, 10),
		persistor:   	persistor,
		log:				[]*LogEntry{},
	}
	for _, peer := range(peers) {
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
	return id, term, isLeader
}

func (worker *Worker) GetState() (int, bool) {
	worker.mux.RLock()
	term := worker.term
	isLeader := worker.isLeader
	worker.mux.RUnlock()
	return term, isLeader
}