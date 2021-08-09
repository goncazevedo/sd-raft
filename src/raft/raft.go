package raft

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state       int       // 0 -> Follower, 1 -> Candidate, 2 -> Leader
	timeout     time.Time // Timeout for RPC requests(candidato ou follower)
	votedFor    int       // Id de quem recebeu o voto, -1 -> nenhum
	currentTerm int       // Termo atual
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

// argumentos do candidato para o pedido de voto
type RequestVoteArgs struct {
	// Request vote RPC
	CdtTerm int // Termo do candidato pedindo voto
	CdtID   int // ID do candidato pedindo voto
}

// resposta de um pedido de voto
type RequestVoteReply struct {
	CurrentTerm int  // termo atual da eleição
	VoteGranted bool // se o voto foi dado
}

// heartbeat para resetar o timeout e informar o termo certo aos seguidores
type HeartbeatArgs struct {
	LeaderID int // Id do lider
	Term     int // Termo do lider
}

// resposta dada pelos peers ao receber um hearbeat
type HeartbeatReply struct {
	Term   int  // Termo do peer
	Sucess bool // Se o peer recebeu o heartbeat com sucesso
}

//Transforma um seguidor em candidato e faz o pedido de voto pros seus peers
func (rf *Raft) startElection() {
	rf.state = 1
	rf.currentTerm++
	savedCurrentTerm := rf.currentTerm
	rf.timeout = time.Now()
	rf.votedFor = rf.me
	fmt.Printf("%d se torna candidato (currentTerm=%d);\n", rf.me, savedCurrentTerm)
	votesReceived := 1

	// Manda o requestVote para todos os peers
	for _, server := range rf.peers {
		go func(server *labrpc.ClientEnd) {
			args := RequestVoteArgs{
				CdtTerm: savedCurrentTerm,
				CdtID:   rf.me,
			}
			var reply RequestVoteReply
			fmt.Printf("Enviando RequestVote para: args=%+v\n", args)
			if ok := server.Call("Raft.RequestVote", &args, &reply); ok == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				fmt.Printf("\trequest Vote reply recebida: %+v\n", reply)
				if rf.state != 1 {
					fmt.Printf("\tenquanto esperava pela resposta, state = %v\n", rf.state)
					return
				}

				if reply.CurrentTerm > savedCurrentTerm {
					fmt.Println("\tresposta do requestVote indica q o termo está desatualizado")
					rf.becomeFollower(reply.CurrentTerm)
					return
				} else if reply.CurrentTerm == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived > len(rf.peers)/2 { // (?)
							// Won the election!
							fmt.Printf("\nEleição termo %d{\n\tlider: %d\n\tvotos: %d\n}\n\n", savedCurrentTerm, rf.me, votesReceived)
							rf.startLeader()
							return
						}
					}
				}
			}
		}(server)
	}

	// Se a eleição não for bem sucedida, inicia uma nova eleição
	go rf.runElectionTimer()
}

//preenche o request vote reply com as informações sobre o voto (quem votou e se votou)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("RequestVote: %+v [currentTerm=%d, votedFor=%d]\n", args, rf.currentTerm, rf.votedFor)

	if args.CdtTerm > rf.currentTerm {
		fmt.Printf("\tTermo de %d foi atualizado no vote request", rf.me)
		rf.becomeFollower(args.CdtTerm)
	}

	if rf.currentTerm == args.CdtTerm && (rf.votedFor == -1 || rf.votedFor == args.CdtID) {
		reply.VoteGranted = true
		rf.votedFor = args.CdtID
		rf.timeout = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.CurrentTerm = rf.currentTerm
	fmt.Printf("\tRequestVote reply: {votedFor:%d , currentTerm: %d, id rf: %d}\n", rf.votedFor, rf.currentTerm, rf.me)
	return
}

// Líder envia o heartbeat para todos os peers
func (rf *Raft) mandaHeartbeats() {
	rf.mu.Lock()
	savedCurrentTerm := rf.currentTerm
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		args := HeartbeatArgs{rf.me, savedCurrentTerm}
		reply := HeartbeatReply{}
		//fmt.Printf("Enviando heartbeat para %d args=%+v\n", i, args)
		go rf.sendHeartbeat(i, &args, &reply)
	}
}

// Lider envia o heartbeat pra um servidor
// envia para o host(server) o args e reply, espera o reply de volta
func (rf *Raft) sendHeartbeat(server int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveHeartbeat", args, reply)
	if ok == true {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// args.Term == savedCurrentTerm sempre
		if reply.Term > args.Term {
			// fmt.Println("A resposta do Heartbeat indica q o termo está desatualizado")
			rf.becomeFollower(reply.Term)
		}
	}
	return ok
}

// manda (Id do lider e o termo) e recebe (termo e Sucesso)
func (rf *Raft) ReceiveHeartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("Heartbeat args de %d: %+v\n", rf.me, args)

	// Atualizo o termo dos seguidores para o mesmo do líder se precisar
	if args.Term > rf.currentTerm {
		// fmt.Printf("\t Termo de %d foi atualizado ao receber heartbeat", rf.me)
		rf.becomeFollower(args.Term)
	}

	// Caso os seguidores estejam no termo do líder reforço que eles são seguidores
	reply.Sucess = false
	if args.Term == rf.currentTerm {
		if rf.state != 0 && rf.me != args.LeaderID {
			rf.becomeFollower(args.Term)
		}
		rf.timeout = time.Now()
		reply.Sucess = true
	}

	reply.Term = rf.currentTerm
	//fmt.Printf("\t Heartbeat reply de %d: %+v\n", rf.me, *reply)
	return
}

// Monitoramento dos timeouts para o inicio de uma nova eleição
func (rf *Raft) runElectionTimer() {
	timeoutDuration := rf.electionTimeout()
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()
	fmt.Printf("%d começou timer (%v), term=%d\n", rf.me, timeoutDuration, termStarted)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		rf.mu.Lock()
		if rf.state != 1 && rf.state != 0 {
			fmt.Printf("durante o timer para iniciar uma eleição o seguidor se elegeu\n")
			rf.mu.Unlock()
			return
		}

		if termStarted != rf.currentTerm {
			fmt.Printf("durante o timer para iniciar uma eleição o termo mudou de %d para %d\n", termStarted, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		// Inicia uma nova eleição caso não haja nenhum lider mandando heartbeats
		// por um tempo(timeoutDuration)
		if elapsed := time.Since(rf.timeout); elapsed >= timeoutDuration {
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond

}

func (rf *Raft) becomeFollower(term int) {
	fmt.Printf("%d virou seguidor no termo %d\n", rf.me, term)
	rf.state = 0
	rf.currentTerm = term
	rf.votedFor = -1
	rf.timeout = time.Now()

	go rf.runElectionTimer()
}

func (rf *Raft) startLeader() {
	rf.state = 2
	//fmt.Printf("%d virou lider no termo =%d\n", rf.me, rf.currentTerm)
	fmt.Printf("Enviando heartbeats para os followers\n")
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// A cada tick manda um heartbeat
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

// precisa setar o timeout de cada peer,
//contar o tempo, caso atinja algum timeout, enviar os pedidos de voto daquele peer
//e depois de enviado fazer a contagem de votos e decidir se precisa de uma nova eleição ou não
//uma vez com um lider eleito
//a cada ciclo de tempo enviar um heartbeat para todos os seguidores
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.peers = peers
	rf.persister = persister

	rf.currentTerm = 0 // só tinha no nosso
	rf.state = 0
	rf.votedFor = -1

	go func() {
		rf.mu.Lock()
		rf.timeout = time.Now()
		rf.mu.Unlock()
		rf.runElectionTimer()
	}()

	return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	Term := -1
	isLeader := true

	// Your code here (2B).

	return index, Term, isLeader
}

func (rf *Raft) Kill() {

}
func (rf *Raft) persist() {
	// Your code here (2C).
}

func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
