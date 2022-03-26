package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	ConfigUpdateTimeout = 100
)
const (
	OK             Err = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeDesired = "ErrTimeDesired"
	ErrConfiguration = "ErrConfiguration"
)

const ExecuteTimeout = 100


type Err string
type OpType string
type ConfigType string
type ShardStatus int

func (ss *ShardStatus) string() string{
	switch *ss {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BePulling:
		return "BePulling"
	case Deleting:
		return "Deleting"
	}
	return ""
}

const (
	Operation ConfigType = "OperationType"
	Configuration = "Configuration"
	InsertShards = "InsertShards"
	DeleteShards = "DeleteShards"
)
const (
	Put OpType = "Put"
	Append = "Append"
	Get = "Get"
)
const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	Deleting
)

type CommandRequest struct {

	OpType
	Key   string
	Value string

	ClientID int64
	CommandId int
}
type CommandReply struct {
	Err   Err
	Value string
}

type SKVStateMachine interface {
	Get(key string)(string,Err)
	Put(key string,value string)Err
	Append(key,value string)Err
}
type Shards struct {
	KVs map[string]string
	status ShardStatus
}


func MakeShards() *Shards  {
	return &Shards{KVs: make(map[string]string),status: Serving}
}

func (s *Shards) Get(key string)(string,Err) {
	if value, ok := s.KVs[key];ok {
		return value,""
	}
	return "",ErrNoKey
}
func (s *Shards) Put(key string,value string)Err {
	s.KVs[key] = value
	return OK
}
func (s *Shards) Append(key string,value string)Err {
	_, err := s.Get(key)
	if err != "" {
		s.KVs[key] = value
	}else {
		s.KVs[key] += value
	}
	return OK
}
