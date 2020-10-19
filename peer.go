package main

import "math/rand"
import "time"
import "fmt"

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
	go worker.revert()
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
	if !worker.isCandidate {
		fmt.Printf("node %d no longer candidate for term %d\n", worker.id, worker.term)
		return
	}
	for len(worker.votes) > 0 {
		vote := <- worker.votes
		if vote.term > worker.term {
			worker.isCandidate = false
			worker.term = vote.term
			fmt.Printf("node %d reverting to follower on term %d\n", worker.id, worker.term)
			return
		}
		if vote.term == term && vote.granted {
			votes += 1
		}
	}
	//check for majority
	fmt.Printf("node %d got %d votes for term %d\n", worker.id, votes, worker.term)
	if (votes > (len(worker.peers)+1)/2) {
		fmt.Printf("node %d is leader for term %d\n", worker.id, worker.term)
		worker.isLeader = true
		worker.isCandidate = false
		worker.term = term
		//reset election timeout
		for _, peer := range(worker.peers) {
			peer.applyCh <- ApplyMsg {entries: nil, term: worker.term, leaderID: worker.id}
		}
	}
}


func (worker *Worker) electionTimeout() {
	rand.Seed(time.Now().UnixNano())
	for {
		if (!worker.isLeader) {
			//wait for leader, then become candidate
			select {
			case <- worker.timeout:
				continue
			//random timeout between 150-300ms
			case <- time.After(time.Duration(rand.Intn(150) + 151) * time.Millisecond) : 
				worker.votedFor = worker.id
				worker.isCandidate = true
				worker.term += 1
				fmt.Printf("node %d becoming candidate for term %d\n", worker.id, worker.term)
				worker.requestVotes(worker.term)
				continue
			}
		}
	}
}

func (worker *Worker) handleMsg() {
	for {
		if (!worker.isLeader) {
			msg := <- worker.applyCh
			if msg.term >= worker.term {
				//message from leader, reset election timeout
				worker.timeout <- 1
				//handle message
			}
		}
	}
}

func (worker *Worker) respondToVotes() {
	highestVoted := 0
	for {
		if (!worker.isLeader && !worker.isCandidate) {
			vote := <- worker.voteinput
			if vote.term > highestVoted {
				highestVoted = vote.term
				worker.votedFor = -1
			}
			fmt.Printf("node %d term %d got vote request from node %d on term %d, curr voted for: %d\n", worker.id, worker.term, vote.from.id, vote.term, worker.votedFor)
			if vote.term > worker.term && 
				(worker.votedFor == -1 || worker.votedFor == vote.from.id) && vote.lastIndex >= worker.commitIndex {
				//grant vote
				worker.term = vote.term
				(vote.from).votes <- VoteResponse{term: worker.term, granted: true}
				worker.votedFor = vote.from.id
				//restart election timer
				worker.timeout <- 1
				fmt.Printf("node %d voting for node %d on term %d\n", worker.id, vote.from.id, vote.term)
			} else {
				(vote.from).votes <- VoteResponse{term: worker.term, granted: false}
			}

		} 
		
	}
}

func (worker *Worker) revert() {
	for {
		if (worker.isCandidate || worker.isLeader) {
			for len(worker.applyCh) > 0 {
				msg := <- worker.applyCh
				if msg.term >= worker.term {
					//revert to follower
					worker.isCandidate = false
					worker.isLeader = false
					worker.term = msg.term
					fmt.Printf("node %d reverting to follower on term %d\n", worker.id, worker.term)
				}	
			}
		}
	}
}

func (worker *Worker) HB() {
	for{
		time.Sleep(X)
		if worker.isLeader {
			fmt.Printf("curr leader: %d, term: %d\n", worker.id, worker.term)
			for _, peer := range(worker.peers) {
				peer.applyCh <- ApplyMsg {entries: nil, term: worker.term, leaderID: worker.id}
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
	return worker.id, worker.term, worker.isLeader
}

func (worker *Worker) GetState() (int, bool) {
	return worker.term, worker.isLeader
}