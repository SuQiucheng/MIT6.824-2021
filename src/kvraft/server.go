package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int

	//lastOperations map[int64]OperationContext
	//determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	stateMachine KVStateMachine
	notifyChan map[int]chan *CommandResponse
	lastApplyCommand map[int64]*CommandContext

}

type KVStateMachine interface {
	Get(key string)(string,Err)
	Put(key string,value string)Err
	Append(key,value string)Err
}
type MemoryKV struct {
	KV map[string]string
}

func MakeMemoryKV() *MemoryKV {
	return &MemoryKV{KV: make(map[string]string)}
}
func (memoryKV *MemoryKV) Get(key string)(string,Err) {
	if value, ok := memoryKV.KV[key];ok {
		return value,""
	}
	return "",NoKeyErr
}
func (memoryKV *MemoryKV) Put(key string,value string)Err {
	memoryKV.KV[key] = value
	return ""
}
func (memoryKV *MemoryKV) Append(key string,value string)Err {
	_, err := memoryKV.Get(key)
	if err != "" {
		memoryKV.KV[key] = value
	}else {
		memoryKV.KV[key] += value
	}
	return ""
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
//判断需要snapshot吗
func (kv *KVServer)CheckSnapshot() bool{
	if kv.maxraftstate!=-1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate{
		DPrintf("kv.rf.GetRaftStateSize():%d >= kv.maxraftstate:%d",kv.rf.GetRaftStateSize(),kv.maxraftstate)
		return true
	}
	return false
}
func (kv *KVServer) TakeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplyCommand)
	//DPrintf("stateMachine : %+v",kv.stateMachine)
	e.Encode(kv.stateMachine)
	data := w.Bytes()
	kv.rf.Snapshot(index,data)
}

func (kv *KVServer) RestoreSnapshot(snapshot []byte)  {
	if snapshot==nil || len(snapshot)==0{
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplyCommand map[int64]*CommandContext
	var stateMachine MemoryKV
	if d.Decode(&lastApplyCommand) != nil || d.Decode(&stateMachine) != nil {
		DPrintf("读取持久化错误")
	}
	kv.lastApplyCommand = lastApplyCommand
	kv.stateMachine = &stateMachine
	DPrintf("RestoreSnapshot finished!lastApplyCommand:%+v,stateMachine:%+v",lastApplyCommand,stateMachine)
}

func (kv *KVServer) ClientRequest(request *CommandRequest,response *CommandResponse){
	kv.mu.Lock()
	//如果是上一次的命令，那么就直接返回
	if request.OpType!=OpGet&&kv.isDuplicateRequest(request.ClientId,request.CommandId){
		DPrintf("isDuplicateRequest!")
		response = kv.lastApplyCommand[request.ClientId].CommandResponse
		kv.mu.Unlock()
		return
	}
	index,_,isLeader := kv.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Start's index is %+v,isLeader:%v,Command:%v",index,isLeader,request)


	time.Now()
	//此处的index是指raft的log的index
	if _,ok := kv.notifyChan[index];!ok{
		kv.notifyChan[index] = make(chan *CommandResponse,1)
	}
	ch := kv.notifyChan[index]
	DPrintf("index %d chan is 阻塞",index)
	//因为这个时候会阻塞
	kv.mu.Unlock()
	select {
	case result := <-ch:
		DPrintf("result<-ch:%v",result)
		response.Err,response.Value = result.Err,result.Value
	case <-time.After(ExecuteTimeout*time.Millisecond):
		response.Err = TimeDesired
	}
	go func(index int) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.notifyChan,index)
		DPrintf("kv.notifyChan is %v",len(kv.notifyChan))
	}(index)
}
func (kv *KVServer) isDuplicateRequest(clientId int64, commandId int64) bool {
	if _,ok:=kv.lastApplyCommand[clientId];ok {
		context := kv.lastApplyCommand[clientId]
		if context.CommandRequest.CommandId==commandId{
			return true
		}
	}
	return false
}
func (kv *KVServer)Applier()  {
	for kv.killed()==false{
		select {
		case message := <-kv.applyCh:
			DPrintf("message is %+v",message)
			//DPrintf("message's Command is %+v",message.Command.(Command).CommandRequest)
			if message.CommandValid{
				kv.mu.Lock()
				//message.CommandIndex是raft log的日志index
				if message.CommandIndex<=kv.lastApplied {
					//假如此时的命令序号小于KVServer的应用号，代表这个命令已经被应用了
					kv.mu.Unlock()
					continue
				}
				response := new(CommandResponse)
				//kv.lastApplied = message.CommandIndex
				command := message.Command.(Command)
				switch command.OpType {
				case OpGet:
					response.Value,response.Err = kv.stateMachine.Get(command.Key)
				case OpPut:
					DPrintf("OpPut")
					if kv.isDuplicateRequest(command.ClientId, command.CommandId) {
						response = kv.lastApplyCommand[command.ClientId].CommandResponse
					}else {
						response.Err = kv.stateMachine.Put(command.Key,command.Value)
						DPrintf("kv.stateMachine is %v",kv.stateMachine)
						kv.lastApplyCommand[command.ClientId] = &CommandContext{command.CommandRequest,response}
					}
				case OpAppend:
					if kv.isDuplicateRequest(command.ClientId, command.CommandId) {
						response = kv.lastApplyCommand[command.ClientId].CommandResponse
					}else {
						response.Err = kv.stateMachine.Append(command.Key,command.Value)
						kv.lastApplyCommand[command.ClientId] = &CommandContext{command.CommandRequest,response}
					}
				}
				if currentTerm,isLeader := kv.rf.GetState();isLeader&&currentTerm==message.CommandTerm{
					kv.notifyChan[message.CommandIndex]<-response
				}
				//这个index是raft的index
				kv.lastApplied = message.CommandIndex
				DPrintf("kv.lastApplied is %v",kv.lastApplied)
				//判断此时需要snapshot吗
				if kv.CheckSnapshot(){
					kv.TakeSnapshot(kv.lastApplied)
				}
				kv.mu.Unlock()
			}else if message.SnapshotValid{
				kv.mu.Lock()
				DPrintf("开始应用Snapshot")
				if message.SnapshotIndex <= kv.lastApplied{
					kv.mu.Unlock()
					continue
				}
				kv.RestoreSnapshot(message.Snapshot)
				kv.rf.CondInstallSnapshot(message.SnapshotTerm,message.SnapshotIndex,message.Snapshot)
				kv.mu.Unlock()
			}else {
				DPrintf("既不是commandValid也不是SnapshotValid")
			}
		}
	}
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.stateMachine = MakeMemoryKV()
	kv.notifyChan = make(map[int]chan *CommandResponse)
	kv.lastApplyCommand = make(map[int64]*CommandContext)

	kv.RestoreSnapshot(persister.ReadSnapshot())
	go kv.Applier()
	return kv
}
