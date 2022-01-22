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
	"6.824/labgob"
	"bytes"
	"fmt"
	"math"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)



//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	applyCond *sync.Cond
	state NodeState

	currentTerm int
	votedFor    int
	logs        []Entry

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	//将提交的日志记录的索引
	commitIndex int
	//已经提交到状态机的最后一个日志的索引
	lastApplied int
	//每次选举后重新初始化
	//leader下一个将要发送到follower的index
	nextIndex []int
	//leader与follower匹配的进度
	matchIndex []int
}
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type NodeState int

const (
	follower NodeState = iota
	candidate
	leader
)


func (state NodeState) String() string {
	switch state {
	case follower:
		return "Follower"
	case candidate:
		return "Candidate"
	case leader:
		return "Leader"
	}
	panic(fmt.Sprintf("unexpected NodeState %d", state))
}

func (rf *Raft) String() string {
	return fmt.Sprintf("&Raft{me:%d,dead:%d,electionTimer:%v,heartbeatTimer:%v}\n", rf.me, rf.dead, rf.electionTimer, rf.heartbeatTimer)
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesResponse struct {
	//当前任期，leader用来更新自己
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
		d.Decode(&logs)!=nil{
	  DPrintf("读取持久化错误")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.logs = logs
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//候选者的任期
	Term         int
	//候选者的编号
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (rf *Raft) genRequestVoteArgs()  *RequestVoteArgs{
	lastLog := rf.GetLastLog()
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
}

func (requestVoteArgs *RequestVoteArgs)String() string {
	return fmt.Sprintf("&RequestVoteArgs{term:%d,candidateId:%d,lastLogIndex:%d,LastLogTerm:%d}\n",requestVoteArgs.Term,requestVoteArgs.CandidateId,requestVoteArgs.LastLogIndex,requestVoteArgs.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	//当前任期，候选者用来更新自己
	Term        int
	//如果候选者当选则为true
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//DPrintf("%d %dis called by %d %d",rf.me,rf.currentTerm,args.CandidateId,args.Term)
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//DPrintf("%d没有投票给%d",rf.me,args.CandidateId)
		return
	}
	//DPrintf("当前rf状态为：%v",rf)
	if args.Term > rf.currentTerm {
		rf.ChangeState(follower)
		rf.currentTerm = args.Term
	}
	//候选者的Item大，但是要从同一个候选者中选到拥有全部已经提交的log记录的peer
	if args.LastLogTerm<rf.GetLastLog().Term ||(args.LastLogTerm==rf.GetLastLog().Term&&args.LastLogIndex<rf.GetLastLog().Index){
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//DPrintf("%d没有投票给%d",rf.me,args.CandidateId)

		return
	}
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true

	rf.persist()
	//DPrintf("%d投票给%d",rf.me,args.CandidateId)

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//DPrintf("ok:%v",ok)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//DPrintf("ok:%v",ok)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//Todo 是不是需要开启一个apply线程，将已经commit但是还没有apply的log应用一下呢？
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	if term,isLeader = rf.GetState();isLeader{
		rf.mu.Lock()
		defer rf.mu.Unlock()
		index = rf.GetLastLog().Index
		//_,index = rf.lastLogTermIndex()
		term = rf.currentTerm
		entry := Entry{
			Command: command,
			Term:    term,
			Index:   index+1,
		}
		rf.logs = append(rf.logs,entry)
		rf.matchIndex[rf.me] = entry.Index
		rf.nextIndex[rf.me] = entry.Index+1

		//DPrintf("the log has appended the %d's log entries!",rf.me)
	//	ToDo 现在就开启一个心跳，还是直接发送给follower
		rf.BroadcastHeartBeat(Replicator)
		rf.persist()
	}
	return index+1, term, isLeader
}

func (rf *Raft) lastLogTermIndex()(int,int)  {
	lastLog := rf.GetLastLog()
	term:= lastLog.Term
	index:= lastLog.Index
	return term,index
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			//DPrintf("%d 's electionTimer is dired!",rf.me)
			rf.mu.Lock()
			rf.ChangeState(candidate)
			rf.currentTerm += 1
			//进行选举
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.persist()
			rf.mu.Unlock()
		//	如果心跳超时仍然要等待它这个term过完吗
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == leader {
				//要向其他的follower发送心跳
				rf.BroadcastHeartBeat(HeartBeat)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}

	}
}

type AppendEntriesType int

const (
	HeartBeat AppendEntriesType = iota
	Replicator
)

//leader通过RPC调用，重置选举超时
func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {
	//log.Printf("follower:%d接收到心跳，重置选举时间", os.Getpid())
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("follower:%d接收到心跳，重置选举时间", rf.me)
	//DPrintf("follower:%d接收到心跳，日志为：%v", rf.me,request)
	if request.Term<rf.currentTerm {
		response.Term = rf.currentTerm
		response.Success = false
		return
	}
	if request.Term>rf.currentTerm{
		rf.currentTerm = request.Term
	}
	rf.ChangeState(follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	_,lastLogIndex := rf.lastLogTermIndex()
	//DPrintf("follower %d's lastLogIndex is %d",rf.me,lastLogIndex)
	if lastLogIndex<request.PrevLogIndex{
		response.Term = rf.currentTerm
		response.Success = false
		return
	}
	if rf.logs[request.PrevLogIndex].Term!=request.PrevLogTerm {
		rf.logs = rf.logs[0:request.PrevLogIndex]
		response.Term = rf.currentTerm
		response.Success = false
		return
	}
	//DPrintf("follower %d start log replicate",rf.me)
	//是不是需要找到unMatchIndex
	rf.logs = rf.logs[:request.PrevLogIndex+1]
	rf.logs = append(rf.logs,request.Entries...)

	rf.persist()

	//DPrintf("follower %d finish log replicate,the entries is %v",rf.me,rf.logs)
	rf.applyCond.Signal()

	if request.LeaderCommit>rf.commitIndex{
		rf.commitIndex = int(math.Min(float64(len(rf.logs)-1), float64(request.LeaderCommit)))
	}
	response.Term = rf.currentTerm
	response.Success = true
	return
}

//可以加速复制，也可以不用多线程的方法复制-------from learnAi
func (rf *Raft) BroadcastHeartBeat(t AppendEntriesType) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		} else {
			//DPrintf("%d 开始日志的复制！",rf.me)
			go rf.replicateOneRound(peer)
		}
	}
}

func (rf *Raft) replicateOneRound(peer int){
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	request := rf.GenAppendEntriesRequest(peer)
	rf.mu.Unlock()
	response := &AppendEntriesResponse{}
	if rf.sendAppendEntries(peer,request,response){
		rf.mu.Lock()
		//DPrintf("%d has the response:%v",rf.me,response)
		rf.handleAppendEntriesResponse(peer,request,response)
		rf.mu.Unlock()
	}
}
func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse){
	if rf.state==leader && rf.currentTerm==request.Term{
		if response.Success {
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			//DPrintf("Node %d,rf.nextIndex:%v",rf.me,rf.nextIndex)

			majorityIndex := rf.getMajorityIndex(rf.matchIndex)
			if majorityIndex > rf.commitIndex {
				rf.commitIndex = majorityIndex
				//DPrintf("leader %d's commitIndex is %d",rf.me,rf.commitIndex)
				rf.applyCond.Signal()
			}
		} else {
			if response.Term > rf.currentTerm {
				rf.ChangeState(follower)
				rf.currentTerm,rf.votedFor = response.Term,-1
			} else {
				preLogIndex := request.PrevLogIndex
				//如果这一次请求增加日志没有匹配上的话，就跳过这个term
				//ToDo 需要优化，这样子的话有可能一下子要传输的log[]太多了
				for preLogIndex > 0 && rf.logs[preLogIndex].Term == request.PrevLogTerm {
					preLogIndex--
				}
				rf.nextIndex[peer] = preLogIndex+1
			}
		}
	}
}
//根据matchIndex，判断出那些log已经被大多数peer记录了
func (rf *Raft) getMajorityIndex(matchIndex []int) int  {
	tmp := make([]int,len(matchIndex))
	copy(tmp,matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	return tmp[len(tmp)/2]
}
func (rf *Raft) GenAppendEntriesRequest(peer int) *AppendEntriesRequest {
	preLogIndex:= rf.nextIndex[peer]-1
	entries := make([]Entry,len(rf.logs[preLogIndex+1:]))
	copy(entries,rf.logs[preLogIndex+1:])
	appendEntriesRequest := &AppendEntriesRequest{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  rf.logs[preLogIndex].Term,
		//存储日志的记录，用来使follower跟随leader
		Entries:      entries,
		//leader的commitIndex
		LeaderCommit: rf.commitIndex,
	}
	return appendEntriesRequest
}

func (rf *Raft) StartElection() {
	//DPrintf("%d开始选举",rf.me)
	request := rf.genRequestVoteArgs()
	//统计选票有多少
	votesNum := 1
	rf.votedFor = rf.me
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteReply)

			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//可能出现选举定时器超时，重启选举的情况，此时rf.currentTerm就发生变化了
				if rf.currentTerm == request.Term && rf.state == candidate {
					if response.VoteGranted {
						votesNum += 1
						if votesNum > len(rf.peers)/2 {
							//增加ChangeState的方法
							rf.ChangeState(leader)
							//发送空的心跳
							rf.BroadcastHeartBeat(HeartBeat)
						}
					} else if response.Term > rf.currentTerm {
						//增加ChangeState方法
						rf.ChangeState(follower)
						//假如有其他当选，或者自己的最后的log没有别人的新，那么就需要将votedFor设置为-1
						rf.currentTerm = response.Term
						rf.votedFor = -1

						rf.persist()
					}
				}
			}
		}(peer)
		//DPrintf("%d的当前的状态为%v,选票有%d张",rf.me,rf.state,votesNum)

	}

}
func (rf *Raft) GetLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) ChangeState(state NodeState) {
	if rf.state == state {
		return
	}
	//DPrintf("{Node %d} changes state from %s to %s in term %d", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
	switch state {
	case follower:
		//rf.votedFor = -1
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomElectionTimeout())
		//DPrintf("%d的选举时间被重置了！",rf.me)
	case candidate:
	case leader:
		//lastLog := rf.GetLastLog()
		//初始化nextIndex,matchIndex
		rf.nextIndex = make([]int,len(rf.peers))
		rf.matchIndex = make([]int,len(rf.peers))
		for i,length:=0,len(rf.peers);i<length;i++{
			//rf.nextIndex[i] = lastLog.Index+1
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
		//DPrintf("Node %d,rf.nextIndex:%v",rf.me,rf.nextIndex)
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}
func (rf *Raft) applier() {
	for rf.killed()==false{
		rf.mu.Lock()
		for rf.lastApplied>=rf.commitIndex{
			rf.applyCond.Wait()
		}
		commitIndex,lastApplied := rf.commitIndex,rf.lastApplied
		entries := make([]Entry,commitIndex-lastApplied)
		copy(entries,rf.logs[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()
		for _,entry := range entries{
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  entry.Index,
				CommandTerm: entry.Term,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = int(math.Max(float64(rf.lastApplied),float64(commitIndex)))
		rf.mu.Unlock()
		//DPrintf("Node %d's entry is applying！",rf.me)
	}

}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh: applyCh,
		state:          follower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(StableHeartbeatTimeout()),
		heartbeatTimer: time.NewTimer(RandomElectionTimeout()),
		commitIndex: 0,
		lastApplied: 0,
	}
	rf.applyCond  = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeatTimer.Stop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	//lastLog := rf.GetLastLog()
	for i:=0;i<len(peers);i++{
		rf.matchIndex[i],rf.nextIndex[i] = 0,len(rf.logs)
	}
	go rf.ticker()
	go rf.applier()
	return rf
}
