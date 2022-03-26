package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	commandId int64
	clientId int64
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.commandId = 0
	ck.clientId = nrand()
	return ck
}
func (ck *Clerk) Command(commandRequest *CommandRequest) *CommandResponse{
	commandRequest.ClientId = ck.clientId
	commandRequest.CommandId = ck.commandId

	for{
		commandResponse := new(CommandResponse)

		if !ck.servers[ck.leaderId].Call("ShardCtrler.RequestHandle", commandRequest, commandResponse)||commandResponse.Err==TimeDesired||commandResponse.WrongLeader == true{
			ck.leaderId = (ck.leaderId+1)%len(ck.servers)
			continue
		}
		DPrintf("leader is %d",ck.leaderId)
		ck.commandId+=1
		return commandResponse
	}
	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		commandResponse := new(CommandResponse)
	//		ok := srv.Call("ShardCtrler.RequestHandle", commandRequest, commandResponse)
	//		if ok && commandResponse.WrongLeader == false&&commandResponse.Err!=TimeDesired {
	//			ck.commandId += 1
	//			return commandResponse
	//		}
	//		//DPrintf("[%d] commandReq is %v,commandRes is %v",index,commandRequest,commandResponse)
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandRequest{}
	// Your code here.
	args.OpType = Query
	args.Num = num

	reply := ck.Command(args)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandRequest{}
	// Your code here.
	args.OpType = Join

	args.Servers = servers

	ck.Command(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandRequest{}
	// Your code here.

	args.OpType = Leave
	args.GIDs = gids

	ck.Command(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandRequest{}
	// Your code here.
	args.OpType = Move
	args.Shard = shard
	args.GID = gid

	ck.Command(args)
}
