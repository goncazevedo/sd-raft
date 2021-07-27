package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
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
	leader      int //Id do lider atual
	currentTerm int //Termo atual

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	isleader = false
	if rf.state == 2 {
		isleader = true
	}

	term = rf.currentTerm

	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

//passa todos os argumentos do candidato representando um pedido de voto
type RequestVoteArgs struct {
	// Request vote RPC
	cdtTerm int //Termo do candidato pedindo voto
	cdtID   int //ID do candidato pedindo voto
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
// passa os argumentos de resposta para um pedido de voto
type RequestVoteReply struct {
	currentTerm int //termo atual da eleição
	voteGranted bool
}

//heartbeat tem que ser uma estrutura separada, que vai conter as informações de lider
type HeartbeatArgs struct {
	leaderID int
	term     int
}

//resposta do heartbeat
type HeartbeatReply struct {
	term   int
	sucess bool
}

//
// example RequestVote RPC handler.
// RAFT::REQUESTVOTE
// Raft.RequestVote(args, reply)

//preenche o request vote reply com as informações sobre o voto (quem votou e se votou)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.cdtTerm {
		rf.currentTerm = args.cdtTerm
		rf.votedFor = args.cdtID
		reply.currentTerm = args.cdtTerm
		rf.timeout = setTimeout()
		reply.voteGranted = true
		fmt.Print("teste: ", reply.currentTerm)
		return
	}

	reply.currentTerm = rf.currentTerm
	reply.voteGranted = false
	fmt.Print("teste: ", reply.currentTerm)
	return
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

func (rf *Raft) mandaHeartbeats() bool {
	sucess := true
	for i := 0; i < len(rf.peers); i++ {
		args := HeartbeatArgs{rf.me, rf.currentTerm}
		reply := HeartbeatReply{}
		rf.sendHeartbeat(i, &args, &reply)
		if !reply.sucess {
			sucess = false
		}
	}
	return sucess
}

func (rf *Raft) pedeVotos(server int, results chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	reply := RequestVoteReply{}
	args := RequestVoteArgs{rf.me, rf.currentTerm}
	rf.sendRequestVote(server, &args, &reply)

	if reply.voteGranted {
		fmt.Println("voto concedido")
		results <- true
	} else {
		fmt.Println("voto negado")
		results <- false
	}
}

// so faz um pedido de voto e retorna se deu certo pra um server especifico
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	print("Raft.peers: ", rf.peers)
	return ok
}

//Lider envia o heartbeat pra um servidor
//envia para o host(server) o args e reply, espera o reply de volta
func (rf *Raft) sendHeartbeat(server int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.receiveHeartbeat", args, reply)
	return ok
}

// manda (Id do lider e o termo) e recebe (termo e sucesso)
// if term < currentTerm return false
//atualizar o follower do lider eleito e enviar pro lider a confirmação
func (rf *Raft) receiveHeartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {

	//Se recebo o termo desatualizado, não reseto timeout, e mando termo certo
	//Se recebo o termo certo, atualiza o termo e lider do raft, envia resposta, reseta timeout do raft
	if args.term < rf.currentTerm {
		reply.sucess = false
		reply.term = rf.currentTerm
		return
	}
	rf.leader = args.leaderID
	rf.state = 0
	rf.currentTerm = args.term
	reply.sucess = true
	reply.term = args.term
	rf.timeout = setTimeout()
}

func setTimeout() time.Time {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	tempo, _ := time.ParseDuration(fmt.Sprintf("0.%[1]ds", r1.Intn(200)+400))
	return time.Now().Add(tempo)
}

func setLeaderTimeout() time.Time {
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

// precisa setar o timeout de cada peer,
//contar o tempo, caso atinja algum timeout, enviar os pedidos de voto daquele peer
//e depois de enviado fazer a contagem de votos e decidir se precisa de uma nova eleição ou não
//uma vez com um lider eleito
//a cada ciclo de tempo enviar um heartbeat para todos os seguidores
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.state = 0
	//provisório pra nulo
	rf.leader = 2
	if rf.me == 2 {
		fmt.Println("I'm the candidate")
		rf.timeout = time.Now()
	}
	// Startar go routine da eleiçõa
	if time.Now().After(rf.timeout) && rf.me == 2 {
		if rf.state == 0 || rf.state == 1 {
			rf.state = 1
			rf.currentTerm++
			rf.timeout = setTimeout()
			fmt.Print("Termo atual = 1\n")
			// pede os votos de todos os peers paralelalmente, criando go routine
			votes := 0
			var wg sync.WaitGroup
			results := make(chan bool, len(rf.peers)-1)
			wg.Add(len(rf.peers) - 1)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.pedeVotos(i, results, &wg)
				}
			}
			// conta os votos de todos os peers paralelalmente, criando go routine
			fmt.Print("Antes do wait\n")
			wg.Wait()
			fmt.Print("Depois do wait\n")
			close(results)
			for resposta := range results {
				if resposta {
					votes++
				}
			}
			fmt.Println("votos recebidos: ")
			fmt.Println(votes)
			//ganhou a eleição
			if votes > len(rf.peers) {
				rf.leader = rf.me
				rf.state = 2
				rf.timeout = setLeaderTimeout()
				rf.mandaHeartbeats()
				//empate
			} else {
				rf.timeout = setTimeout()
			}
		} else {
			rf.mandaHeartbeats()
		}

	}
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
