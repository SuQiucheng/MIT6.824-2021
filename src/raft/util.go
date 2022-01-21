package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = true

type electionRand struct {
	mu sync.Mutex
	randTime *rand.Rand
}
var eRand =&electionRand{
	randTime: rand.New(rand.NewSource(time.Now().UnixNano())),
}


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	HeartbeatTimeout = 50
	ElectionTimeout = 500
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout)*time.Millisecond
}

func RandomElectionTimeout() time.Duration{
	eRand.mu.Lock()
	defer eRand.mu.Unlock()
	return time.Duration(ElectionTimeout+eRand.randTime.Intn(ElectionTimeout))*time.Millisecond
}
