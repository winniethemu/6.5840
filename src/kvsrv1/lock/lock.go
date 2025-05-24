package lock

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	mu sync.Mutex

	// Lock state
	owner string
	key   string
	done  chan bool
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	id := kvtest.RandValue(8)
	lk := &Lock{ck: ck, owner: id, key: l}
	return lk
}

func (lk *Lock) Acquire() {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	val, ver, err := lk.ck.Get(lk.key)
	if err == rpc.ErrNoKey {
		lk.ck.Put(lk.key, lk.owner, 0)
		return
	}
	if val == "" {
		lk.ck.Put(lk.key, lk.owner, ver)
		return
	}

	<-lk.done
	lk.ck.Put(lk.key, lk.owner, ver)
}

func (lk *Lock) Release() {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	val, ver, err := lk.ck.Get(lk.key)
	if err == rpc.ErrNoKey || val == "" {
		panic("cannot release an empty lock")
	}
	if val != lk.owner {
		panic("cannot release someone else's lock")
	}
	lk.ck.Put(lk.key, "", ver)

	// Does this work in a distributed env? Channels are in-memory
	lk.done <- true
}
