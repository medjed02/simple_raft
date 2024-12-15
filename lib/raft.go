package simple_raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const (
	LeaderRole    = iota
	FollowerRole  = iota
	CandidateRole = iota
)

const (
	heartbeatPeriodMs = 1000
)

type LogEntry struct {
	Command  string
	Term     uint64
	Callback func()
}

type ReplicaInfo struct {
	Address     string
	HttpAddress string
	Vote        int
	NextIndex   int
	MatchIndex  int
	RpcClient   *rpc.Client
}

type StateMachine interface {
	Apply(cmd string) error
}

type ConsensusServer struct {
	Id      int
	Address string

	// for rpc
	server *http.Server

	stateLock sync.Mutex

	currentTerm uint64
	log         []LogEntry

	// leaderRole | followerRole | candidateRole
	leaderId int
	role     int

	stateMachine    StateMachine
	replicas        map[int]ReplicaInfo
	electionTimeout time.Time
	commitIndex     int
	lastApplied     int
}

func (s *ConsensusServer) replicaRpc(id int, funcName string, req, rsp any) bool {
	s.stateLock.Lock()
	replicaInfo := s.replicas[id]
	rpcClient := replicaInfo.RpcClient
	var err error
	if rpcClient == nil {
		replicaInfo.RpcClient, err = rpc.DialHTTP("tcp", replicaInfo.Address)
		rpcClient = replicaInfo.RpcClient
	}
	s.stateLock.Unlock()

	if err == nil {
		err = rpcClient.Call(funcName, req, rsp)
	}

	return err == nil
}

func (s *ConsensusServer) tryToBecomeLeader() bool {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if s.role != CandidateRole {
		return false
	}

	votesCount := 0
	for id := range s.replicas {
		if s.replicas[id].Vote == s.Id {
			votesCount++
		}
	}

	if votesCount >= len(s.replicas)/2+1 {
		for id, info := range s.replicas {
			info.NextIndex = len(s.log)
			info.MatchIndex = 0
			s.replicas[id] = info
		}
		s.log = append(s.log, LogEntry{Term: s.currentTerm, Command: ""})
		s.role = LeaderRole
		return true
	}
	return false
}

func (s *ConsensusServer) checkLeaderHeartbeat() bool {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return !time.Now().After(s.electionTimeout)
}

func (s *ConsensusServer) updateElectionTimeout() {
	bound := heartbeatPeriodMs * 2
	newTimeoutInterval := time.Duration((bound + rand.Intn(bound)) * int(time.Millisecond))
	s.electionTimeout = time.Now().Add(newTimeoutInterval)
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

func (s *ConsensusServer) getVote() int {
	for id, info := range s.replicas {
		if id == s.Id {
			return info.Vote
		}
	}
	return 0
}

func (s *ConsensusServer) vote(candidate int) {
	for id, info := range s.replicas {
		if id == s.Id {
			info.Vote = candidate
			s.replicas[id] = info
			break
		}
	}
}

func (s *ConsensusServer) tryUpdateTerm(term uint64) bool {
	if term > s.currentTerm {
		s.currentTerm = term
		s.role = FollowerRole
		s.vote(0)
		s.updateElectionTimeout()
		return true
	}
	return false
}

func (s *ConsensusServer) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	s.tryUpdateTerm(req.Term)

	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	if req.Term < s.currentTerm || s.getVote() != 0 {
		return nil
	}

	lastLogIndex := len(s.log) - 1
	lastLogTerm := s.log[lastLogIndex].Term
	logOk := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	fmt.Println("Vote for", req.CandidateId)
	if logOk {
		s.vote(req.CandidateId)
		rsp.VoteGranted = true
	}

	return nil
}

func (s *ConsensusServer) requestVote() {
	for id, info := range s.replicas {
		if id == s.Id {
			continue
		}
		go func(s *ConsensusServer) {
			s.stateLock.Lock()
			lastLogIndex := len(s.log) - 1
			lastLogTerm := s.log[len(s.log)-1].Term
			term := s.currentTerm
			s.stateLock.Unlock()

			req := RequestVoteRequest{
				Term:         term,
				CandidateId:  s.Id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			var rsp RequestVoteResponse
			if !s.replicaRpc(id, "ConsensusServer.HandleRequestVoteRequest", req, &rsp) {
				return
			}

			s.stateLock.Lock()
			defer s.stateLock.Unlock()
			if s.tryUpdateTerm(rsp.Term) || s.currentTerm != rsp.Term {
				return
			}
			if rsp.VoteGranted {
				info.Vote = s.Id
				s.replicas[id] = info
			}
		}(s)
	}
}

func (s *ConsensusServer) runForElection() {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	if s.role == LeaderRole {
		return
	}

	s.role = CandidateRole
	s.currentTerm++
	for id, info := range s.replicas {
		if id == s.Id {
			info.Vote = s.Id
		} else {
			info.Vote = 0
		}
		s.replicas[id] = info
	}

	s.updateElectionTimeout()
	s.requestVote()
}

type AppendEntriesRequest struct {
	Term         uint64
	PrevLogIndex int
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit int
	LeaderId     int
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

func (s *ConsensusServer) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	s.tryUpdateTerm(req.Term)
	if req.Term == s.currentTerm {
		s.role = FollowerRole
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	if s.role != FollowerRole || req.Term < s.currentTerm {
		return nil
	}

	s.leaderId = req.LeaderId
	s.updateElectionTimeout()

	if !(req.PrevLogIndex == 0 || (req.PrevLogIndex < len(s.log) && s.log[req.PrevLogIndex].Term == req.PrevLogTerm)) {
		return nil
	}
	if len(s.log) != 0 && len(s.log) >= req.PrevLogIndex {
		s.log = s.log[:req.PrevLogIndex+1]
	}
	s.log = append(s.log, req.Entries...)
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, len(s.log)-1)
	}

	rsp.Success = true
	return nil
}

func (s *ConsensusServer) appendEntries() {
	for id := range s.replicas {
		if id == s.Id {
			continue
		}

		go func(s *ConsensusServer) {
			s.stateLock.Lock()
			info := s.replicas[id]
			next := s.replicas[id].NextIndex
			prevLogIndex := next - 1
			prevLogTerm := s.log[prevLogIndex].Term
			var entries []LogEntry
			if len(s.log) >= next+1 {
				entries = s.log[next:]
			}
			term := s.currentTerm
			commit := s.commitIndex

			s.stateLock.Unlock()

			req := AppendEntriesRequest{
				Term:         term,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commit,
				LeaderId:     s.Id,
			}

			var rsp AppendEntriesResponse
			if !s.replicaRpc(id, "ConsensusServer.HandleAppendEntriesRequest", req, &rsp) {
				return
			}

			s.stateLock.Lock()
			defer s.stateLock.Unlock()

			if s.tryUpdateTerm(rsp.Term) || (s.role == LeaderRole && rsp.Term != req.Term) {
				return
			}

			info = s.replicas[id]
			if rsp.Success {
				info.NextIndex = req.PrevLogIndex + len(entries) + 1
				info.MatchIndex = info.NextIndex - 1
			} else {
				info.NextIndex = max(info.NextIndex-1, 1)
			}
			s.replicas[id] = info
		}(s)
	}
}

func (s *ConsensusServer) tryOpenNewTerm() bool {
	if !s.checkLeaderHeartbeat() {
		s.runForElection()
		return true
	}
	return false
}

func (s *ConsensusServer) tryCommit() {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	if s.role == LeaderRole {
		for i := len(s.log) - 1; i >= s.commitIndex; i-- {
			replicasAplliedCount := 0
			for id, info := range s.replicas {
				if id == s.Id || info.MatchIndex >= i {
					replicasAplliedCount++
				}
			}
			if replicasAplliedCount >= len(s.replicas)/2+1 {
				s.commitIndex = i
				break
			}
		}
	}

	for s.lastApplied <= s.commitIndex {
		logEntry := s.log[s.lastApplied]
		if len(logEntry.Command) != 0 {
			s.stateMachine.Apply(logEntry.Command)
			if logEntry.Callback != nil {
				logEntry.Callback()
			}
		}
		s.lastApplied++
	}
}

func (s *ConsensusServer) ApplyCommand(command string) bool {
	s.stateLock.Lock()

	if s.role != LeaderRole {
		s.stateLock.Unlock()
		return false
	}

	var wg sync.WaitGroup
	wg.Add(1)
	newEntry := LogEntry{
		Term:    s.currentTerm,
		Command: command,
		Callback: func() {
			wg.Done()
		},
	}
	s.log = append(s.log, newEntry)

	s.stateLock.Unlock()

	s.appendEntries()
	wg.Wait()

	return true
}

func (s *ConsensusServer) GetGoodReplica() string {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	if s.role != LeaderRole {
		return ""
	}

	maxMatchIndex := 0
	var goodReplicaIds []int
	for id, info := range s.replicas {
		if info.MatchIndex > maxMatchIndex {
			maxMatchIndex = info.MatchIndex
			goodReplicaIds = []int{id}
		} else if info.MatchIndex == maxMatchIndex {
			goodReplicaIds = append(goodReplicaIds, id)
		}
	}
	fmt.Println("find replicas", maxMatchIndex, goodReplicaIds)
	if maxMatchIndex < s.commitIndex {
		return ""
	}
	if len(goodReplicaIds) != 0 {
		goodReplica := goodReplicaIds[rand.Intn(len(goodReplicaIds))]
		return s.replicas[goodReplica].HttpAddress
	}
	return ""
}

func (s *ConsensusServer) GetCommitIndex() int {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	return s.commitIndex
}

func (s *ConsensusServer) GetLeader() string {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	fmt.Println("no, leader is", s.leaderId)
	return s.replicas[s.leaderId].HttpAddress
}

func (s *ConsensusServer) Launch() {
	s.stateLock.Lock()
	s.role = FollowerRole
	s.stateLock.Unlock()

	s.log = append(s.log, LogEntry{})

	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)
	s.server = &http.Server{Handler: mux}
	go s.server.Serve(listener)

	go func(s *ConsensusServer) {
		for {
			s.stateLock.Lock()
			nowRole := s.role
			fmt.Println("My role is", s.role, ", current term is", s.currentTerm)
			s.stateLock.Unlock()

			switch nowRole {
			case LeaderRole:
				s.appendEntries()
			case CandidateRole:
				if !s.tryToBecomeLeader() {
					s.tryOpenNewTerm()
				}
			case FollowerRole:
				s.tryOpenNewTerm()
			}

			s.tryCommit()

			time.Sleep(heartbeatPeriodMs * time.Millisecond)
		}
	}(s)
}

func NewServer(
	replicasList map[int]ReplicaInfo,
	stateMachine StateMachine,
	id int,
) *ConsensusServer {
	replicas := make(map[int]ReplicaInfo)
	for id, info := range replicasList {
		replicas[id] = info
	}
	return &ConsensusServer{
		Id:           id,
		Address:      replicas[id].Address,
		replicas:     replicas,
		stateMachine: stateMachine,
		stateLock:    sync.Mutex{},
	}
}
