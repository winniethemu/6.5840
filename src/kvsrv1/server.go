package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

type Record struct {
	Value   string
	Version rpc.Tversion
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]*Record
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]*Record),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	record, ok := kv.data[args.Key]
	if ok {
		reply.Value = record.Value
		reply.Version = rpc.Tversion(record.Version)
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	record, ok := kv.data[args.Key]
	if ok {
		if record.Version == rpc.Tversion(args.Version) {
			record.Value = args.Value
			record.Version = args.Version + 1
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.data[args.Key] = &Record{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
