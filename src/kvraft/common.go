package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
)

const ExecuteTimeout = 2000

type Err string

const (
	NoKeyErr Err = "NoKeyErr"
	TimeDesired Err = "TimeDesired"
	ErrWrongLeader Err = "ErrWrongLeader"
)
type Command struct {
	*CommandRequest
}

type CommandRequest struct {
	Key string
	Value string
	OpType OpType
	ClientId int64
	CommandId int64
}

type CommandResponse struct {
	Value string
	Err Err
}
type CommandContext struct {
	*CommandRequest
	*CommandResponse
}
