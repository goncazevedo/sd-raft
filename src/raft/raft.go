package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state       int       // 0 -> Follower, 1 -> Candidate, 2 -> Leader
	timeout     time.Time // Timeout for RPC requests(candidato ou follower)
	votedFor    int
	currentTerm int //Termo atual

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var Term int
	var isleader bool

	isleader = false
	if rf.state == 2 {
		isleader = true
	}

	Term = rf.currentTerm

	return Term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

//passa todos os argumentos do candidato representando um pedido de voto
type RequestVoteArgs struct {
	// Request vote RPC
	CdtTerm int //Termo do candidato pedindo voto
	CdtID   int //ID do candidato pedindo voto
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
// passa os argumentos de resposta para um pedido de voto
type RequestVoteReply struct {
	CurrentTerm int //termo atual da eleição
	VoteGranted bool
}

//heartbeat tem que ser uma estrutura separada, que vai conter as informações de lider
type HeartbeatArgs struct {
	LeaderID int
	Term     int
}

//resposta do heartbeat
type HeartbeatReply struct {
	Term   int
	Sucess bool
}

//
// example RequestVote RPC handler.
// RAFT::REQUESTVOTE
// Raft.RequestVote(args, reply)

//preenche o request vote reply com as informações sobre o voto (quem votou e se votou)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	/* 	rf.mu.Lock()
	   	defer rf.mu.Unlock()

	   	if rf.currentTerm < args.CdtTerm {
	   		rf.setTimeout()
	   		rf.currentTerm = args.CdtTerm
	   		rf.state = 0
	   		rf.votedFor = args.CdtID
	   		reply.CurrentTerm = args.CdtTerm
	   		reply.VoteGranted = true
	   		fmt.Println("termo ", args.CdtTerm, "candidato: ", args.CdtID, "folower: ", rf.me, " votou")
	   		return
	   	}
	   	reply.CurrentTerm = rf.currentTerm
	   	reply.VoteGranted = false
	   	fmt.Println("termo ", rf.currentTerm, "candidato: ", args.CdtID, "folower: ", rf.me, " rejeitou")
	   	return */
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("RequestVote: %+v [currentTerm=%d, votedFor=%d]\n", args, rf.currentTerm, rf.votedFor)

	if args.CdtTerm > rf.currentTerm {
		fmt.Println("... term out of date in RequestVote")
		rf.becomeFollower(args.CdtTerm)
	}

	if rf.currentTerm == args.CdtTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CdtID) {
		reply.VoteGranted = true
		rf.votedFor = args.CdtID
		rf.timeout = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.CurrentTerm = rf.currentTerm
	fmt.Printf("... %+v RequestVote reply: {votedFor:%d , currentTerm: %d, id rf: %d}\n", args, rf.votedFor, rf.currentTerm, rf.me)
	return
}

func (rf *Raft) becomeFollower(term int) {
	fmt.Printf("%d becomes Follower with term=%d\n", rf.me, term)
	rf.state = 0
	rf.currentTerm = term
	rf.votedFor = -1
	rf.timeout = time.Now()

	go rf.runElectionTimer()
}

func (rf *Raft) startLeader() {
	rf.state = 2
	fmt.Printf("becomes Leader; term=%d\n", rf.currentTerm)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			rf.mandaHeartbeats()
			<-ticker.C

			rf.mu.Lock()
			if rf.state != 2 {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) mandaHeartbeats() {
	// rf.mu.Lock()
	// savedCurrentTerm := rf.currentTerm
	// rf.mu.Unlock()
	// Sucess := true
	// for i := 0; i < len(rf.peers); i++ {
	// 	args := HeartbeatArgs{rf.me, savedCurrentTerm}
	// 	reply := HeartbeatReply{}
	// 	if i != rf.me {
	// 		rf.sendHeartbeat(i, &args, &reply)
	// 	}
	// }
	// return Sucess
	rf.mu.Lock()
	savedCurrentTerm := rf.currentTerm
	rf.mu.Unlock()
	for _, server := range rf.peers {
		args := HeartbeatArgs{
			Term:     savedCurrentTerm,
			LeaderID: rf.me,
		}
		go func(server *labrpc.ClientEnd) {
			fmt.Printf("sending AppendEntries: ni=%d, args=%+v\n", 0, args)
			var reply HeartbeatReply
			if ok := server.Call("Raft.ReceiveHeartbeat", &args, &reply); ok == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					fmt.Println("term out of date in heartbeat reply")
					rf.becomeFollower(reply.Term)
					return
				}
			}
		}(server)
	}
}

func (rf *Raft) startElection() {
	rf.state = 1
	rf.currentTerm++
	savedCurrentTerm := rf.currentTerm
	rf.timeout = time.Now()
	rf.votedFor = rf.me
	fmt.Printf("becomes Candidate (currentTerm=%d);\n", savedCurrentTerm)
	votesReceived := 1

	// Send RequestVote RPCs to all other servers concurrently.
	for _, server := range rf.peers {
		go func(server *labrpc.ClientEnd) {
			args := RequestVoteArgs{
				CdtTerm: savedCurrentTerm,
				CdtID:   rf.me,
			}
			var reply RequestVoteReply
			fmt.Printf("sending RequestVote: %+v\n", args)
			if ok := server.Call("Raft.RequestVote", &args, &reply); ok == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				fmt.Printf("received RequestVoteReply %+v\n", reply)
				if rf.state != 1 {
					fmt.Printf("while waiting for reply, state = %v\n", rf.state)
					return
				}

				if reply.CurrentTerm > savedCurrentTerm {
					fmt.Println("term out of date in RequestVoteReply")
					rf.becomeFollower(reply.CurrentTerm)
					return
				} else if reply.CurrentTerm == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(rf.peers)+1 {
							// Won the election!
							fmt.Printf("wins election with %d votes\n", votesReceived)
							rf.startLeader()
							return
						}
					}
				}
			}
		}(server)
	}

	// Run another election timer, in case this election is not successful.
	go rf.runElectionTimer()
}

func (rf *Raft) electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond

}

func (rf *Raft) runElectionTimer() {
	timeoutDuration := rf.electionTimeout()
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()
	fmt.Printf("%d começou timer (%v), term=%d\n", rf.me, timeoutDuration, termStarted)
	// This loops until either:
	// - we discover the election timer is no longer needed, or
	// - the election timer expires and this rf becomes a candidate
	// In a follower, this typically keeps running in the background for the
	// duration of the rf's lifetime.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		rf.mu.Lock()
		if rf.state != 1 && rf.state != 0 {
			fmt.Printf("in election timer state=%d, bailing out\n", rf.state)
			rf.mu.Unlock()
			return
		}

		if termStarted != rf.currentTerm {
			fmt.Printf("in election timer term changed from %d to %d, bailing out\n", termStarted, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		if elapsed := time.Since(rf.timeout); elapsed >= timeoutDuration {
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) pedeVotos(server int, results chan bool) {
	/* 	defer wg.Done() */
	reply := RequestVoteReply{}
	args := RequestVoteArgs{rf.currentTerm, rf.me}
	rf.sendRequestVote(server, &args, &reply)

	if reply.VoteGranted {
		results <- true
	} else {
		results <- false
	}
}

// so faz um pedido de voto e retorna se deu certo pra um server especifico
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//Lider envia o heartbeat pra um servidor
//envia para o host(server) o args e reply, espera o reply de volta
func (rf *Raft) sendHeartbeat(server int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveHeartbeat", args, reply)
	return ok
}

// manda (Id do lider e o termo) e recebe (termo e Sucesso)
// if Term < CurrentTerm return false
//atualizar o follower do lider eleito e enviar pro lider a confirmação
func (rf *Raft) ReceiveHeartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	/* println(args.LeaderID, " enviou heartbeat ", rf.me) */
	//Se recebo o termo desatualizado, não reseto timeout, e mando termo certo
	//Se recebo o termo certo, atualiza o termo e lider do raft, envia resposta, reseta timeout do raft
	/* if args.Term < rf.currentTerm {
		reply.Sucess = false
		reply.Term = rf.currentTerm
		return
	}
	rf.setTimeout()
	rf.becomeFollower(args.Term)
	reply.Sucess = true
	reply.Term = args.Term */
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("AppendEntries: %+v\n", args)

	if args.Term > rf.currentTerm {
		fmt.Printf("... term out of date in AppendEntries\n")
		rf.becomeFollower(args.Term)
	}

	reply.Sucess = false
	if args.Term == rf.currentTerm {
		if rf.state != 0 && rf.me != args.LeaderID {
			rf.becomeFollower(args.Term)
		}
		rf.timeout = time.Now()
		reply.Sucess = true
	}

	reply.Term = rf.currentTerm
	fmt.Printf("AppendEntries reply: %+v\n", *reply)
	return
}

func (rf *Raft) setTimeout() {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	segundo := 0         // 0 -> 1.0
	ms := r1.Intn(6) + 3 // 0.5 -> 0.95
	tempo, _ := time.ParseDuration(fmt.Sprintf("%d.%ds", segundo, ms))
	// 0.5 1.5
	// fmt.Println(rf.me, "timeout: ", tempo)
	rf.timeout = time.Now().Add(tempo)
}

func setHeartbeatTimeout() time.Time {
	tempo, _ := time.ParseDuration("0.100s")
	return time.Now().Add(tempo)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// LOCALHOST:8080 8081 8082
//peers[0] = 8080 peers[1] = 8081

func (rf *Raft) eleicao() {
	for true {
		if time.Now().After(rf.timeout) || time.Now().Equal(rf.timeout) {
			if rf.state == 0 || rf.state == 1 {
				rf.state = 1
				rf.currentTerm++
				fmt.Println("Termo atual = ", rf.currentTerm, "Candidato = ", rf.me, "timeout = ", rf.timeout)
				// pede os votos de todos os peers paralelalmente, criando go routine
				votes := 1
				rf.votedFor = rf.me

				/* var wg sync.WaitGroup */
				results := make(chan bool, len(rf.peers))
				/* wg.Add(len(rf.peers)) */
				for i := 0; i < len(rf.peers); i++ {
					/* rf.pedeVotos(i, results, &wg) */
					if i != rf.me {
						rf.pedeVotos(i, results)
					}

				}
				// conta os votos de todos os peers paralelalmente, criando go routine
				/* wg.Wait() */
				rf.mu.Lock()
				close(results)
				for resposta := range results {
					if resposta {
						votes++
					}
				}
				fmt.Println("votos recebidos termo(", rf.currentTerm, ") candidato(", rf.me, "): ", votes, "/", len(rf.peers))
				//ganhou a eleição
				if votes > len(rf.peers)/2 && rf.state == 1 {
					rf.mandaHeartbeats()
					fmt.Println("me elegi ", rf.me, ") termo ", rf.currentTerm)
					rf.state = 2
					rf.timeout = setHeartbeatTimeout()
					//empate
				} else {
					rf.setTimeout()
				}
				rf.mu.Unlock()
			} else {
				rf.mandaHeartbeats()
				rf.timeout = setHeartbeatTimeout()
			}
		}
	}
}

// precisa setar o timeout de cada peer,
//contar o tempo, caso atinja algum timeout, enviar os pedidos de voto daquele peer
//e depois de enviado fazer a contagem de votos e decidir se precisa de uma nova eleição ou não
//uma vez com um lider eleito
//a cada ciclo de tempo enviar um heartbeat para todos os seguidores
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	/* rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.state = 0
	if rf.me == 0 {
		tempo, _ := time.ParseDuration("0.300s")
		rf.timeout = time.Now().Add(tempo)
	}
	if rf.me == 1 {
		tempo, _ := time.ParseDuration("0.800s")
		rf.timeout = time.Now().Add(tempo)
	}
	if rf.me == 2 {
		tempo, _ := time.ParseDuration("1.200s")
		rf.timeout = time.Now().Add(tempo)
	}
	rf.setTimeout()

	go rf.eleicao()

	return rf */
	rf := &Raft{}
	rf.me = me
	rf.peers = peers
	rf.persister = persister
	rf.state = 0
	rf.votedFor = -1

	go func() {
		// The rf is quiescent until ready is signaled; then, it starts a countdown
		// for leader election.
		rf.mu.Lock()
		rf.timeout = time.Now()
		rf.mu.Unlock()
		rf.runElectionTimer()
	}()

	return rf
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	Term := -1
	isLeader := true

	// Your code here (2B).

	return index, Term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
