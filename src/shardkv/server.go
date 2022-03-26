package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"


const Debug = false

func DPrintf(s string,args ...interface{}) (n int,err error) {
	if Debug{
		log.Printf(s,args...)
	}
	return
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ConfigType
	*CommandRequest
	*ConfigurationChangeCommand
}

type CommandContext struct {
	ConfigType
	CommandId int
	*CommandReply
}

type ConfigurationChangeCommand struct {
	//ToDo
	Config *shardctrler.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	sm       *shardctrler.Clerk
	lastConfig shardctrler.Config
	config   shardctrler.Config

	dead int32
	lastApplied int
	lastContext map[int64]*CommandContext
	notifyChan map[int]chan *CommandReply
	sKVStateMachine map[int]*Shards
}
func (kv *ShardKV) isDuplicated(clientId int64, requestId int) bool {
	context, ok := kv.lastContext[clientId]
	return ok && requestId == context.CommandId
}

func (kv *ShardKV) RequestHandle(args *CommandRequest, reply *CommandReply) {
	kv.mu.Lock()

	shard := key2shard(args.Key)
	DPrintf("RequestHandle:kv's config is %+v,request is %+v",kv.config,args)
	if !kv.canServe(shard){
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	//如果是上一次的command，那么就可以直接返回
	if args.OpType != Get && kv.isDuplicated(args.ClientID,args.CommandId) {
		reply = kv.lastContext[args.ClientID].CommandReply
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		ConfigType:                 Operation,
		CommandRequest:             args,
		ConfigurationChangeCommand: nil,
	}
	kv.SubmitToRaft(op,reply)

}

func (kv *ShardKV) SubmitToRaft(op Op,reply *CommandReply) {
	index,_,isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	time.Now()
	if _,ok:=kv.notifyChan[index];!ok{
		kv.notifyChan[index] = make(chan *CommandReply,1)
	}
	ch := kv.notifyChan[index]
	kv.mu.Unlock()

	select {
	case result := <- ch:
		reply.Err = result.Err
		reply.Value = result.Value

	case <-time.After(ExecuteTimeout * time.Millisecond):
		reply.Err = ErrTimeDesired
	}

	go func(index int) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.notifyChan,index)
	}(index)

}

func (kv *ShardKV) Applier() {
	for kv.Killed()==false{
		select {
		case message := <-kv.applyCh:
			if message.CommandValid{
				kv.mu.Lock()

				if message.CommandIndex<=kv.lastApplied{
					kv.mu.Unlock()
					continue
				}
				op := message.Command.(Op)
				reply := new(CommandReply)

				switch op.ConfigType {
				case Operation:
					args := op.CommandRequest
					kv.applyOperation(args,reply)
				case Configuration:
					args := op.ConfigurationChangeCommand
					kv.applyConfiguration(args,reply)
				}

				if currentTerm,isLeader := kv.rf.GetState();currentTerm==message.CommandTerm&&isLeader{
					kv.notifyChan[message.CommandIndex] <- reply
				}
				kv.lastApplied = message.CommandIndex

				kv.mu.Unlock()
			}
		}

	}
}

func (kv *ShardKV) applyOperation(args *CommandRequest, reply *CommandReply) {
	key := args.Key
	shard := key2shard(key)

	if _,ok := kv.sKVStateMachine[shard];!ok{
		kv.sKVStateMachine[shard] = MakeShards()
	}

	if kv.canServe(shard){
		if kv.isDuplicated(args.ClientID,args.CommandId){
			reply = kv.lastContext[args.ClientID].CommandReply
			return
		}else {
			switch args.OpType {
			case Get:
				kv.Get(args,reply)
			case Put:
				fallthrough
			case Append:
				kv.PutAppend(args,reply)
				kv.lastContext[args.ClientID] = &CommandContext{Operation,args.CommandId,reply}
			}
		}
	}else {
		reply.Err = ErrWrongGroup
	}
	return
}

func (kv *ShardKV) canServe(shard int)  bool{
	return kv.config.Shards[shard]==kv.gid && (kv.sKVStateMachine[shard].status ==Serving ||kv.sKVStateMachine[shard].status ==Deleting)
}


func (kv *ShardKV) Get(args *CommandRequest, reply *CommandReply) {
	// Your code here.
	key := args.Key
	shard := key2shard(key)
	reply.Value,reply.Err = kv.sKVStateMachine[shard].Get(key)
}

func (kv *ShardKV) PutAppend(args *CommandRequest, reply *CommandReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	shard := key2shard(key)

	switch args.OpType {
	case Put:
		kv.sKVStateMachine[shard].Put(key,value)
	case Append:
		kv.sKVStateMachine[shard].Append(key,value)
	}
}

func (kv *ShardKV) ConfigUpdate() {
	for kv.Killed()==false{
		canChangeConfiguration := true
		kv.mu.Lock()
		for _,shard := range kv.sKVStateMachine{
			if shard.status!=Serving{
				canChangeConfiguration = false
				break
			}
		}
		lastConfig := kv.config

		kv.mu.Unlock()
		//DPrintf("%v 更新分片的状态",!canChangeConfiguration)
		if canChangeConfiguration{
			if _, isLeader := kv.rf.GetState();isLeader{
				newConfig := kv.sm.Query(lastConfig.Num+1)
				if newConfig.Num == lastConfig.Num+1{

					cfChangeCommand := ConfigurationChangeCommand{&newConfig}

					op := Op{
						ConfigType:                 Configuration,
						CommandRequest:             nil,
						ConfigurationChangeCommand: &cfChangeCommand,
					}
					reply := new(CommandReply)
					kv.SubmitToRaft(op,reply)
				}
			}
		}
		time.Sleep(ConfigUpdateTimeout*time.Millisecond)
	}
}
func (kv *ShardKV) applyConfiguration(cfChangeCommand *ConfigurationChangeCommand,reply *CommandReply) {
	cf := cfChangeCommand.Config
	// 对于状态机来说，如果这个config不是上一个config的下一个，那么就不能更新
	if cf.Num == kv.config.Num+1{
		kv.updateShardsStatus(kv.config,*cf)
		kv.lastConfig = kv.config
		kv.config = *cf
		reply.Err = OK
		return
	}
	reply.Err = ErrConfiguration
	return
}

func (kv *ShardKV) updateShardsStatus(lastConfig, newConfig shardctrler.Config) {
	//DPrintf("更新分片状态前:%+v，更新分片状态后:%+v",lastConfig,newConfig)
	for s := range newConfig.Shards{
		newGID := newConfig.Shards[s]
		oldGID := lastConfig.Shards[s]
		if newGID==oldGID{
			continue
		}
		if oldGID==0 && newGID == kv.gid{
			if _,ok:=kv.sKVStateMachine[s];!ok{
				kv.sKVStateMachine[s] = MakeShards()
			}
		}else if newGID==kv.gid && oldGID!=kv.gid{
			if _,ok:=kv.sKVStateMachine[s];!ok{
				kv.sKVStateMachine[s] = MakeShards()
			}
			//ToDo Pulling
			//kv.sKVStateMachine[s].status = Pulling
		}else if newGID!=kv.gid && oldGID==kv.gid{
			if _,ok:=kv.sKVStateMachine[s];!ok{
				kv.sKVStateMachine[s] = MakeShards()
			}
			//ToDo BePulling
			//kv.sKVStateMachine[s].status = BePulling
		}
	}
}
//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead,1)
}

func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z==1
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(CommandRequest{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.dead = 0
	kv.lastApplied = 0
	kv.lastContext = make(map[int64]*CommandContext)
	kv.notifyChan = make(map[int]chan *CommandReply)
	kv.sm = shardctrler.MakeClerk(ctrlers)


	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sKVStateMachine = make(map[int]*Shards)
	go kv.Applier()
	go kv.ConfigUpdate()


	return kv
}
