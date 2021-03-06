package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"runtime"
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
	*InsertShardsCommand
	*DeleteShardsCommand
	*EmptyLogCommand
}

type CommandContext struct {
	ConfigType
	CommandId int
	*CommandReply
}

func (c *CommandContext) DeepCopy() *CommandContext {
	return &CommandContext{
		ConfigType:   c.ConfigType,
		CommandId:    c.CommandId,
		CommandReply: &CommandReply{
			Err:   c.CommandReply.Err,
			Value: c.CommandReply.Value,
		},
	}
}

type ConfigurationChangeCommand struct {
	Config *shardctrler.Config
}

type InsertShardsCommand struct {
	ConfigNum int
	ShardsContent map[int]Shards
	ShardsLastOperation map[int64]*CommandContext
}

type DeleteShardsCommand struct {
	ConfigNum int
	ShardsIndex []int
}
type EmptyLogCommand struct {

}

type ShardKV struct {
	dead         int32
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	sm         *shardctrler.Clerk
	lastConfig shardctrler.Config
	config     shardctrler.Config

	lastApplied      int
	lastApplyCommand map[int64]*CommandContext
	notifyChan       map[int]chan *CommandReply
	sKVStateMachine  map[int]*Shards
}
func (kv *ShardKV) isDuplicated(clientId int64, requestId int) bool {
	context, ok := kv.lastApplyCommand[clientId]
	//ToDo why this is  <= rather than ==
	return ok && requestId <= context.CommandId
}

func (kv *ShardKV) RequestHandle(args *CommandRequest, reply *CommandReply) {
	kv.mu.Lock()

	//如果是上一次的command，那么就可以直接返回
	if args.OpType != Get && kv.isDuplicated(args.ClientID,args.CommandId) {
		reply = kv.lastApplyCommand[args.ClientID].CommandReply
		kv.mu.Unlock()
		return
	}
	shard := key2shard(args.Key)
	//DPrintf("[%d-%d]:RequestHandle:kv's config is %+v\n,request is %+v",kv.gid,kv.me,kv.config,args)
	if !kv.canServe(shard){
		//DPrintf("[%d-%d]:kv.canServe false,shard GID ->kv GID is %v->%v,",kv.gid,kv.me,kv.config.Shards[shard],kv.gid)
		reply.Err = ErrWrongGroup
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
					kv.applyOperation(args, reply)
				case Configuration:
					args := op.ConfigurationChangeCommand
					kv.applyConfiguration(args, reply)
				case InsertShards:
					args := op.InsertShardsCommand
					kv.applyInsertShards(args,reply)
				case DeleteShards:
					args := op.DeleteShardsCommand
					kv.applyDeleteShards(args,reply)
				case EmptyLogOpera:

				}

				if currentTerm,isLeader := kv.rf.GetState();currentTerm==message.CommandTerm&&isLeader{
					kv.notifyChan[message.CommandIndex] <- reply
				}
				kv.lastApplied = message.CommandIndex
				if kv.CheckSnapshot(){
					kv.TakeSnapshot(kv.lastApplied)
				}

				kv.mu.Unlock()
			}else if message.SnapshotValid{
				kv.mu.Lock()
				if message.SnapshotIndex<=kv.lastApplied{
					kv.mu.Unlock()
					continue
				}
				//这个是恢复上层应用的快照
				kv.RestoreSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				//这个是恢复下层raft的state，将它的Term、Index都重新设置
				kv.rf.CondInstallSnapshot(message.SnapshotTerm,message.SnapshotIndex,message.Snapshot)
				kv.mu.Unlock()
			}
		}

	}
}

func (kv *ShardKV) applyOperation(args *CommandRequest, reply *CommandReply) {
	key := args.Key
	shard := key2shard(key)

	//if _,ok := kv.sKVStateMachine[shard];!ok{
	//	kv.sKVStateMachine[shard] = MakeShards()
	//}

	if kv.canServe(shard){
		if kv.isDuplicated(args.ClientID,args.CommandId){
			reply = kv.lastApplyCommand[args.ClientID].CommandReply
			return
		}else {
			switch args.OpType {
			case Get:
				kv.Get(args,reply)
			case Put:
				fallthrough
			case Append:
				kv.PutAppend(args,reply)
				kv.lastApplyCommand[args.ClientID] = &CommandContext{Operation,args.CommandId,reply}
			}
			reply.Err = OK
		}
	}else {
		reply.Err = ErrWrongGroup
	}
	return
}

func (kv *ShardKV) canServe(shard int)  bool{
	return kv.config.Shards[shard]==kv.gid && (kv.sKVStateMachine[shard].Status ==Serving ||kv.sKVStateMachine[shard].Status ==Deleting)
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
		//DPrintf("[%d-%d]:PutAppend sKVStateMachine[%v]=%v",kv.gid,kv.me,shard,kv.sKVStateMachine[shard])
		kv.sKVStateMachine[shard].Put(key,value)
	case Append:
		kv.sKVStateMachine[shard].Append(key,value)
	}
	//DPrintf("[%d-%d]:After PutAppend,sKVStateMachine[%v]=%v",kv.gid,kv.me,shard,kv.sKVStateMachine[shard])
}
func (kv *ShardKV) DataMigration() {
	for kv.Killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			//gid:shards
			needPull := map[int][]int{}
			for shardIndex, shard := range kv.sKVStateMachine {
				if shard.Status == Pulling {
					DPrintf("[%d-%d]:shard{%v} is Pulling",kv.gid,kv.me,shardIndex)
					lastGid := kv.lastConfig.Shards[shardIndex]
					needPull[lastGid] = append(needPull[lastGid], shardIndex)
				}
			}

			var wg sync.WaitGroup
			for key, value := range needPull {
				//ToDo why use WaitGroup and go func()
				wg.Add(1)
				go func(servers []string,configNum int,key int,value []int) {
					defer wg.Done()
					requestArgs := &RequestShardsArgs{
						ConfigNum:   configNum,
						Gid:         key,
						ShardsIndex: value,
					}
					var requestReply *RequestShardsReply
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						requestReply = new(RequestShardsReply)
						ok := srv.Call("ShardKV.RequestShardsHandle", requestArgs, requestReply)
						DPrintf("[%d-%d]:RequestShardsHandle call %d-%d is %v,requestArgs:%+v,requestReply:%+v", kv.gid, kv.me, key, si, ok, requestArgs,requestReply)
						if ok && requestReply.Err == OK {
							shardsContent := map[int]Shards{}
							for shardId, shard := range requestReply.ShardsContent {
								shardsContent[shardId] = *shard
							}
							insertShardsCommand := &InsertShardsCommand{requestArgs.ConfigNum, shardsContent,requestReply.ShardsLastOperation}
							op := Op{
								ConfigType:                 InsertShards,
								CommandRequest:             nil,
								ConfigurationChangeCommand: nil,
								InsertShardsCommand:        insertShardsCommand,
							}
							reply := new(CommandReply)
							kv.SubmitToRaft(op, reply)
							DPrintf("[%d-%d]:DataMigration requestArgs:%+v,requestReply:%+v",kv.gid,kv.me,op.InsertShardsCommand,reply)
						}

					}
				}(kv.lastConfig.Groups[key],kv.config.Num,key,value)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(DataMigration * time.Millisecond)
	}
}

func (kv *ShardKV) RequestShardsHandle(args *RequestShardsArgs, reply *RequestShardsReply) {
	_,isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err =ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	//ToDo why is >,rather than !=
	if args.ConfigNum > kv.config.Num{
		//DPrintf("[%d-%d]:kv.killed() = %v,args is %v,kv.config is %v",kv.gid,kv.me,kv.Killed(),args,kv.config)
		reply.Err = ErrConfiguration
		return
	}
	newShardsContent := map[int]*Shards{}
	for _,shardIndex := range args.ShardsIndex{
		newShardsContent[shardIndex] = kv.sKVStateMachine[shardIndex].DeepCopy()
	}

	newShardsLastOperation := map[int64]*CommandContext{}
	for index,operation := range kv.lastApplyCommand{
		newShardsLastOperation[index] = operation.DeepCopy()
	}
	reply.ShardsContent = newShardsContent
	reply.ShardsLastOperation = newShardsLastOperation
	reply.Err = OK
	DPrintf("[%d-%d]:RequestShardsHandle,args is %v,reply.ShardsContent is %v",kv.gid,kv.me,args,reply.ShardsContent)
}

func (kv *ShardKV) applyInsertShards(insertCommand *InsertShardsCommand,reply *CommandReply) {
	//DPrintf("[%d-%d]:applyInsertShards,args:%v",kv.gid,kv.me,insertCommand)
	if insertCommand.ConfigNum != kv.config.Num{
		reply.Err = ErrConfiguration
		return
	}
	for shardIndex,shard := range insertCommand.ShardsContent{
		kv.sKVStateMachine[shardIndex] = (&shard).DeepCopy()
		kv.sKVStateMachine[shardIndex].Status = Deleting
		DPrintf("[%d-%d]:After applyInsertShards,shard[%d] status is Deleting",kv.gid,kv.me,shardIndex)
	}
	for index,operation := range insertCommand.ShardsLastOperation{
		if _,ok:=kv.lastApplyCommand[index];!ok || (ok&&kv.lastApplyCommand[index].CommandId<operation.CommandId){
			kv.lastApplyCommand[index] = operation
		}
	}
	reply.Err = OK
	return

}

func (kv *ShardKV) ConfigUpdate() {
	for kv.Killed()==false{
		if _,isLeader := kv.rf.GetState();isLeader {
			canChangeConfiguration := true
			kv.mu.Lock()
			for _, shard := range kv.sKVStateMachine {
				//ToDo delete state BePulling
				if shard.Status != Serving {
					canChangeConfiguration = false
					break
				}
			}
			lastConfig := kv.config

			DPrintf("[%d-%d]:更新状态：%v,kv.config is %v", kv.gid, kv.me, canChangeConfiguration, kv.config)
			if !canChangeConfiguration{
				for index, shard := range kv.sKVStateMachine {
					DPrintf("[%d-%d]:shard[%d] status:%v",kv.gid, kv.me,index,shard.Status)
				}
				DPrintf("[%d-%d]:更新状态失败,config:%v,lastConfig:%v", kv.gid, kv.me, kv.config,kv.lastConfig)
			}
			kv.mu.Unlock()

			//DPrintf("%v 更新分片的状态",!canChangeConfiguration)
			if canChangeConfiguration {
				newConfig := kv.sm.Query(lastConfig.Num + 1)
				if newConfig.Num == lastConfig.Num+1 {
					cfChangeCommand := ConfigurationChangeCommand{&newConfig}
					op := Op{
						ConfigType:                 Configuration,
						CommandRequest:             nil,
						ConfigurationChangeCommand: &cfChangeCommand,
					}
					reply := new(CommandReply)
					kv.SubmitToRaft(op, reply)
				}
			}
		}
		time.Sleep(ConfigUpdateTimeout*time.Millisecond)
	}
}
func (kv *ShardKV) applyConfiguration(cfChangeCommand *ConfigurationChangeCommand,reply *CommandReply) {
	cf := cfChangeCommand.Config
	DPrintf("[%d-%d]:applyConfiguration,kv.config is %v,config is %v",kv.gid,kv.me,kv.config,cf)
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
				//DPrintf("sKVStateMachine is %v",kv.sKVStateMachine)
			}
		}else if newGID==kv.gid && oldGID!=kv.gid{
			if _,ok:=kv.sKVStateMachine[s];!ok{
				kv.sKVStateMachine[s] = MakeShards()
			}
			kv.sKVStateMachine[s].Status = Pulling
		}else if newGID!=kv.gid && oldGID==kv.gid{
			if _,ok:=kv.sKVStateMachine[s];!ok{
				kv.sKVStateMachine[s] = MakeShards()
			}
			kv.sKVStateMachine[s].Status = BePulling
		}
	}
}

func (kv *ShardKV) DataDeleting()  {
	for kv.Killed() == false{
		if _,isLeader := kv.rf.GetState();isLeader{
			kv.mu.Lock()
			//gid:shards
			needDelete := map[int][]int{}
			for shardIndex,shard := range kv.sKVStateMachine{
				if shard.Status == Deleting{
					DPrintf("[%d-%d]:shard{%v} is Deleting",kv.gid,kv.me,shardIndex)
					lastGid := kv.lastConfig.Shards[shardIndex]
					needDelete[lastGid] = append(needDelete[lastGid],shardIndex)
				}
			}
			var wg sync.WaitGroup
			for key,value := range needDelete{
				wg.Add(1)
				go func(servers []string,configNum int,key int,value []int) {
					defer wg.Done()
					requestArgs := &RequestShardsArgs{
						ConfigNum: configNum,
						Gid:         key,
						ShardsIndex: value,
					}
					var requestReply *RequestShardsReply
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						requestReply = new(RequestShardsReply)
						ok := srv.Call("ShardKV.RequestDeleteShardsHandle", requestArgs, requestReply)
						DPrintf("[%d-%d]:RequestDeleteShardsHandle call is %v,requestArgs is %+v,requestReply is %+v", kv.gid, kv.me, ok, requestArgs,requestReply)
						if ok && requestReply.Err == OK {
							deleteShardsCommand := &DeleteShardsCommand{
								ConfigNum:   requestArgs.ConfigNum,
								ShardsIndex: requestArgs.ShardsIndex,
							}
							op := Op{
								ConfigType:                 DeleteShards,
								CommandRequest:             nil,
								ConfigurationChangeCommand: nil,
								InsertShardsCommand:        nil,
								DeleteShardsCommand:        deleteShardsCommand,
							}
							reply := new(CommandReply)
							kv.SubmitToRaft(op, reply)
							//DPrintf("[%d-%d]:deleteShardsCommand SubmitToRaft,request is %+v,reply is %+v", kv.gid, kv.me,op.DeleteShardsCommand,reply)
						}
					}
				}(kv.lastConfig.Groups[key],kv.config.Num,key,value)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(DataDeleting*time.Microsecond)
	}
}
func (kv *ShardKV) applyDeleteShards(deleteCommand *DeleteShardsCommand, reply *CommandReply) {
	DPrintf("[%d-%d]:applyDeleteShards,deleteCommand is %v,kv.config is %v",kv.gid,kv.me,deleteCommand,kv.config)

	if deleteCommand.ConfigNum == kv.config.Num{
		for _,shardIndex := range deleteCommand.ShardsIndex{
			shard := kv.sKVStateMachine[shardIndex]
			if shard.Status == Deleting{
				kv.sKVStateMachine[shardIndex].Status = Serving
			}else if shard.Status == BePulling{
				kv.sKVStateMachine[shardIndex] = MakeShards()
			}
		}
	}
	//ToDo 无论是否相同，都返回ok
	//DPrintf("[%d-%d]:applyDeleteShards Success!",kv.gid,kv.me)
	reply.Err= OK
	return

}

func (kv *ShardKV) RequestDeleteShardsHandle(args *RequestShardsArgs, reply *RequestShardsReply) {
	_,isLeader := kv.rf.GetState()
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if args.Gid!=kv.gid{
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.ConfigNum < kv.config.Num{
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//leader删除之后，需要将删除的这个log发送到follower上
	deleteShardsCommand := &DeleteShardsCommand{
		ConfigNum:   args.ConfigNum,
		ShardsIndex: args.ShardsIndex,
	}
	op := Op{
		ConfigType:                 DeleteShards,
		CommandRequest:             nil,
		ConfigurationChangeCommand: nil,
		InsertShardsCommand:        nil,
		DeleteShardsCommand:        deleteShardsCommand,
	}
	commandReply := new(CommandReply)
	kv.SubmitToRaft(op,commandReply)
	reply.Err = commandReply.Err
	return
}

func (kv *ShardKV) checkRF() {
	for {
		_,isLeader := kv.rf.GetState()
		DPrintf("[%d-%d]:kv.killed=%v,kv.leader=%v,runtime:%v",kv.gid,kv.me,kv.Killed(),isLeader,runtime.NumGoroutine())
		time.Sleep(1000*time.Millisecond)
	}
}

//当rf重新选leader之后，可能会导致之前未提交的依然没有提交，所以加一个空日志
func (kv *ShardKV) checkEntryInCurrentTerm() {
	for kv.Killed()==false{
		if _,isLeader := kv.rf.GetState();isLeader{
			if !kv.rf.HasLogInCurrentTerm(){
				op := Op{
					ConfigType:                 EmptyLogOpera,
					CommandRequest:             nil,
					ConfigurationChangeCommand: nil,
					InsertShardsCommand:        nil,
					DeleteShardsCommand:        nil,
					EmptyLogCommand:            &EmptyLogCommand{},
				}
				commandReply := new(CommandReply)
				kv.SubmitToRaft(op,commandReply)
			}
		}
		time.Sleep(EmptyLogCheck*time.Millisecond)
	}
}
//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead,1)
	kv.rf.Kill()
	DPrintf("[%d-%d]:kill",kv.gid, kv.me)

}

func (kv *ShardKV) Killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) CheckSnapshot() bool {
	if kv.maxraftstate!=-1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate{
		return true
	}
	return false
}

func (kv *ShardKV) TakeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplyCommand)
	e.Encode(kv.sKVStateMachine)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	data := w.Bytes()
	kv.rf.Snapshot(index,data)
}

func (kv *ShardKV) RestoreSnapshot(snapshot []byte) {
	if snapshot==nil || len(snapshot)==0{
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplyCommand map[int64]*CommandContext
	var sKVStateMachine map[int]*Shards
	var config shardctrler.Config
	var lastConfig shardctrler.Config

	if d.Decode(&lastApplyCommand) != nil || d.Decode(&sKVStateMachine) != nil ||d.Decode(&config)!=nil||d.Decode(&lastConfig)!=nil{
		DPrintf("读取持久化错误！")
	}
	kv.lastApplyCommand = lastApplyCommand
	kv.sKVStateMachine = sKVStateMachine
	kv.config = config
	kv.lastConfig = lastConfig
	DPrintf("[%d-%d]:RestoreSnapshot finished!",kv.gid,kv.me)

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
	kv.lastApplyCommand = make(map[int64]*CommandContext)
	kv.notifyChan = make(map[int]chan *CommandReply)
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.config = shardctrler.DefaultConfig()
	kv.lastConfig = shardctrler.DefaultConfig()

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sKVStateMachine = make(map[int]*Shards)

	kv.RestoreSnapshot(persister.ReadSnapshot())
	go kv.Applier()
	go kv.ConfigUpdate()
	go kv.DataMigration()
	go kv.DataDeleting()
	go kv.checkEntryInCurrentTerm()
	go kv.checkRF()
	DPrintf("[%d-%d]:StartServer kf:%+v",kv.gid, kv.me,kv)

	return kv
}
