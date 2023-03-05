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
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// 当服务提交后，发送确认消息来执行动作
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 日志
type LogEntry struct {
	Index int
	Term  int
}

// Server state
type State struct {
	// 所有服务器公有(非持久化)
	// ======
	// 已经被提交的索引
	// commitIndex int
	// 已经被apply的索引
	// lastApplied int
	// ======

	// 只有Leader有(非持久化)
	// nextIndex []int
	// matchIndex []int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久化信息
	// ======
	// 当前term，默认0
	currentTerm int
	// 这一轮投票的candidateId
	votedFor int
	// 本地日志数组
	log []LogEntry
	// ======

	// 选举是否过期，需要重新选举
	isElectionTimeout bool
	// 当前的leaderId
	leaderId int32
}

const (
	// election timeout range, in millisecond
	electionTimeoutMax = 2500
	electionTimeoutMin = 1250

	heartbeatInterval = 125
	noVote            = -1
)

// 这里必须要传指针，因为不然的话会复制一把锁
func (rf *Raft) IsLeader() bool {
	// 假设leaderId就是自己，则这里会更新成功，并返回true
	return atomic.CompareAndSwapInt32(&rf.leaderId, int32(rf.me), int32(rf.me))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.IsLeader()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// 发送心跳
func (rf *Raft) heartbeat() {
	mu := &rf.mu
	for !rf.killed() && rf.IsLeader() {
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			mu.Lock()
			term := rf.currentTerm
			me := rf.me
			mu.Unlock()
			args := AppendEntriesArgs{term, me}
			var reply AppendEntriesReply
			if ok := rf.sendAppendEntries(peer, &args, &reply); ok {
				// log.Printf("[%d] Get heartbeat reply from %d: %v", me, peer, reply)
				// double check term
				mu.Lock()
				if !reply.Success && reply.Term > term {
					// TODO fallback to follower
				}
				mu.Unlock()
			}
		}
		time.Sleep(heartbeatInterval * time.Microsecond)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// 心跳参数
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

// 心跳返回
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	mu := &rf.mu
	mu.Lock()
	defer mu.Unlock()

	term := rf.currentTerm
	if args.Term < term || rf.killed() {
		return
	}

	// 如果对方的term大于自己，则follow对方
	if args.Term > term {
		rf.currentTerm = args.Term
		rf.votedFor = noVote
	}

	me := rf.me
	log.Printf("%d Got vote asking for: %d\n", me, args.CandidateId)
	// 这一轮没有投过票，则投给他
	// 也有可能这一轮收到了多次同一个candidate的请求，则再投给他
	if rf.votedFor == noVote || rf.votedFor == args.CandidateId {
		log.Printf("%d Voting for %d ...", me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	mu := &rf.mu
	mu.Lock()
	defer mu.Unlock()
	term := rf.currentTerm
	reply.Term = rf.currentTerm
	if args.Term < term {
		reply.Success = false
	} else {
		rf.currentTerm = term
		rf.votedFor = noVote
		reply.Success = true
		// 把自己变成follower
		// 不再选举
		rf.isElectionTimeout = false
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
		// 什么时候需要开始选举?
		// 达到了选举超时时间，即一段时间没有收到其他leader的消息
		mu := &rf.mu
		mu.Lock()
		if rf.isElectionTimeout {
			// 开始选举
			// 1. 增加选举周期
			rf.currentTerm++
			log.Printf("[%d:%d] Reach election timeout, call election now\n", rf.currentTerm, rf.me)

			// 2. 发起投票
			// 发起过程中可能收到其他leader的心跳，如果term大于等于自己的term，则自己不再发起，转而变为follower
			term := rf.currentTerm
			me := rf.me
			mu.Unlock()

			voteRequest := RequestVoteArgs{
				Term:        term,
				CandidateId: me,
			}
			// 3. 遍历服务器列表，发起投票
			ch := make(chan bool)
			for peer := range rf.peers {
				go func(peer int) {
					if peer == me {
						mu.Lock()
						// 这一轮投票投给自己
						rf.votedFor = me
						mu.Unlock()
						ch <- true
					} else {
						// RPC申请投票给自己
						var reply RequestVoteReply
						ok := rf.sendRequestVote(peer, &voteRequest, &reply)
						log.Printf("[%d:%d] Get requestVote response from %d, reply: %t:%v\n", me, term, peer, ok, reply)
						if !ok {
							ch <- false
							return
						}

						// 自己的term小于投票者的term
						// 取消申请，把自己变成follower
						if term < reply.Term {
							log.Printf("[%d:%d] self term small then reply's term: [%d:%d], fallback to follower", term, me, term, reply.Term)
							mu.Lock()
							rf.leaderId = int32(peer)
							rf.currentTerm = reply.Term
							mu.Unlock()
							ch <- false
							return
						}

						// 判断其他机器是否投票给自己
						if reply.VoteGranted {
							ch <- true
						} else {
							ch <- false
						}
					}
				}(peer)
			}

			total := len(rf.peers)
			votes := 0
			for i := 0; i < total; i++ {
				voteForMe := <-ch
				if voteForMe {
					votes++
				}
			}

			if votes > total/2 {
				log.Printf("[%d] Win the election, votes detail: %d/%d", me, votes, total)
				// 赢得了大部分投票，自己晋升为leader
				atomic.StoreInt32(&rf.leaderId, int32(me))
				go rf.heartbeat()
			}
		} else {
			mu.Unlock()
		}

		mu.Lock()
		// 当自己不是leader, 则要重新选举
		rf.isElectionTimeout = !rf.IsLeader()
		mu.Unlock()

		// 随机延迟
		randTime := rand.Intn(electionTimeoutMax - electionTimeoutMin)
		time.Sleep(time.Duration(randTime+electionTimeoutMin) * time.Millisecond)
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.isElectionTimeout = false
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	// 服务启动后，轮询自己的状态，如果是follower且一段时间没有收到心跳，则应该发起选举

	// initialize from state persisted before a crash
	// 如果服崩溃了，在这里恢复数据
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 如果没有收到消息，则这里会发起选举
	go rf.ticker()

	return rf
}
