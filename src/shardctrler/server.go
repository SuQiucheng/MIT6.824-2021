package shardctrler


import (
	"6.824/raft"
	"log"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32
	lastCommand map[int64]*CommandContext
	lastApplied int
	configs []Config // indexed by config num

	notifyChan map[int]chan *CommandResponse
}


func (sc *ShardCtrler) RequestHandle(args *CommandRequest,reply *CommandResponse)  {

	sc.mu.Lock()
	if args.OpType!=Query && sc.isDuplicateRequest(args.ClientId,args.CommandId){
		reply = sc.lastCommand[args.ClientId].CommandResponse
		sc.mu.Unlock()
		return
	}

	index, _, leader := sc.rf.Start(Command{args})
	if !leader{
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("Enter RequestHandle! command's index is %d!command is %+v",index,args)

	if _,ok := sc.notifyChan[index];!ok{
		sc.notifyChan[index] = make(chan *CommandResponse,1)
	}

	ch := sc.notifyChan[index]
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Config = result.Config
		reply.Err = result.Err
		reply.WrongLeader = result.WrongLeader
		DPrintf("reply is %v",reply)
	case <-time.After(ExecuteTimeout * time.Millisecond):
		reply.Err = TimeDesired
	}
	go func(index int) {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		delete(sc.notifyChan,index)
	}(index)
}

func (sc *ShardCtrler)getLastConfig() Config  {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler)Applier() {
	for sc.killed()==false{
		select {
		case message := <-sc.applyCh:
			if message.CommandValid{
				sc.mu.Lock()
				if message.CommandIndex<=sc.lastApplied{
					sc.mu.Unlock()
					continue
				}
				reply := new(CommandResponse)
				command := message.Command.(Command)
				//检查是不是上一个命令，防止重复运行
				if command.OpType!=Query && sc.isDuplicateRequest(command.ClientId,command.CommandId){
					reply = sc.lastCommand[command.ClientId].CommandResponse
				}
				DPrintf("Applier %+v",command.OpType)
				switch command.OpType {
				case Join:
					config := sc.join(command)
					sc.configs = append(sc.configs,config)
					DPrintf("Join finish,configs are %v",sc.configs[len(sc.configs)-1])
					//log.Printf("Join finish,configs are %v",sc.configs[len(sc.configs)-1])

				case Query:
					reply.Config = sc.Query(command)
					DPrintf("Query finish,reply.Config is %v",reply.Config)
					//log.Printf("Query finish,reply.Config is %v",reply.Config)

				case Move:
					config := sc.Move(command)
					sc.configs = append(sc.configs,config)
					DPrintf("Move finish,configs are %v",sc.configs[len(sc.configs)-1])
					//log.Printf("Move finish,configs are %v",sc.configs[len(sc.configs)-1])

				case Leave:
					config := sc.Leave(command)
					sc.configs = append(sc.configs,config)
					DPrintf("Leave finish,configs are %v",sc.configs[len(sc.configs)-1])
					//log.Printf("Leave finish,configs are %v",sc.configs[len(sc.configs)-1])

				}
				if command.OpType!=Query{
					sc.lastCommand[command.ClientId] = &CommandContext{
						OpType:          command.OpType,
						CommandRequest:  command.CommandRequest,
						CommandResponse: reply,
					}
				}
				currentTerm, isLeader := sc.Raft().GetState()
				DPrintf("currentTerm is %d,message.CommandTerm is %d,isLeader is %v,",currentTerm,message.CommandTerm,isLeader)
				if isLeader && currentTerm == message.CommandTerm {
					sc.notifyChan[message.CommandIndex]<- reply
				}
				sc.lastApplied = message.CommandIndex
				sc.mu.Unlock()
			}
		}

	}
}
func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	if _,ok:=sc.lastCommand[clientId];ok{
		context := sc.lastCommand[clientId]
		if context.CommandRequest.CommandId==commandId{
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) join(command Command)  Config{
	groups  := command.Servers
	lastConfig := sc.getLastConfig()
	newGroups := deepCopy(lastConfig.Groups)

	for gid,servers := range groups{
		if _,ok:=newGroups[gid];!ok{
			newGroups[gid] = servers
		}
	}

	config := Config{
		Num:    len(sc.configs),
		Shards: lastConfig.Shards,
		Groups: newGroups,
	}
	g2s := sc.gid2Shard(config)
	for {
		//source,des
		maxGid,minGid := sc.findMaxGid(g2s),sc.findMinGid(g2s)
		if maxGid!=0 && len(g2s[maxGid])-len(g2s[minGid])<=1 {
			break
		}
		g2s[minGid] = append(g2s[minGid],g2s[maxGid][0])
		g2s[maxGid] = g2s[maxGid][1:]
	}

	newShards := new([NShards]int)
	//分片移动，平均，生成新的config
	for gid,shards := range g2s{
		for _,shard := range shards{
			newShards[shard] = gid
		}
	}
	config.Shards = *newShards
	return config
}
func (sc *ShardCtrler) Query(command Command) Config {
	num := command.Num
	if num == -1 ||num>=len(sc.configs){
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) Move(command Command) Config {
	shard:= command.Shard
	gId := command.GID

	lastConfig := sc.getLastConfig()
	newShards := lastConfig.Shards
	newGroups := deepCopy(lastConfig.Groups)


	newShards[shard] = gId

	config := Config{
		Num:    len(sc.configs),
		Shards: newShards,
		Groups: newGroups,
	}
	return config
}
func (sc *ShardCtrler) Leave(command Command) Config {
	lastConfig := sc.getLastConfig()
	newGroups := deepCopy(lastConfig.Groups)
	newShards := lastConfig.Shards

	leaveGIds := command.GIDs

	for _,ele := range leaveGIds{
		if ele==2 {
			continue
		}
	}
	restShards := make([]int,0)

	config := Config{
		Num:    len(sc.configs),
		Shards: newShards,
		Groups: newGroups,
	}

	g2s := sc.gid2Shard(config)


	for _,leaveGid := range leaveGIds{
		if _,ok:=config.Groups[leaveGid];ok{
			delete(config.Groups,leaveGid)
		}

		if _,ok :=g2s[leaveGid];ok{
			restShards = append(restShards,g2s[leaveGid]...)
			delete(g2s,leaveGid)
		}
	}
	if len(config.Groups) != 0 {
		for _,shard := range restShards{
			minGid := sc.findMinGid(g2s)
			config.Shards[shard] = minGid
			g2s[minGid] = append(g2s[minGid],shard)
		}
	}else {
		config.Shards = [NShards]int{}
	}
	return config
}
func (sc *ShardCtrler)gid2Shard(config Config) map[int][]int {
	var g2S map[int][]int = make(map[int][]int,1)
	//必须需要根绝config的Groups初始化g2s，因为可能有些group并没有分配shard
	for k := range config.Groups{
		g2S[k] = make([]int,0)
	}

	for s,g := range config.Shards{
		g2S[g] = append(g2S[g],s)
	}
	return g2S
}
//由于g2s是map，是无序的，所以如果一开始没有对其key排序，那么最终得到的结果在不同的machine上是不一致的
func (sc *ShardCtrler) findMaxGid(g2s map[int][]int) int {
	if s,ok := g2s[0];ok && len(s)>0{
		return 0
	}
	var allGId []int
	for gid := range g2s{
		allGId = append(allGId,gid)
	}
	sort.Ints(allGId)

	index,maxValue := -1,-1

	for _,gid:= range allGId{
		if len(g2s[gid])>maxValue {
			index = gid
			maxValue = len(g2s[gid])
		}
	}
	return index
}

func (sc *ShardCtrler) findMinGid(g2s map[int][]int) int {
	index,minValue := -1,NShards+1

	var allGId []int
	for gid := range g2s{
		allGId = append(allGId,gid)
	}
	sort.Ints(allGId)

	for _,gid:= range allGId{
		if gid!=0 && len(g2s[gid])<minValue {
			index = gid
			minValue = len(g2s[gid])
		}
	}
	return index
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}



//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	atomic.StoreInt32(&sc.dead,1)
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed()bool  {
	return 1==atomic.LoadInt32(&sc.dead)
}



// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Command{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastCommand = make(map[int64]*CommandContext)
	sc.lastApplied = 0
	sc.notifyChan = make(map[int]chan *CommandResponse)


	go sc.Applier()
	return sc
}
