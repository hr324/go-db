package btree

import (
	"sync"
	"sync/atomic"
)

type readGuard struct {
	db   *KV
	seq  uint64
	id   uint64
	done uint32
}

type dbCC struct {
	wmu     sync.Mutex
	mu      sync.Mutex
	readers map[uint64]uint64
	nextID  uint64
	seqFn   func(*KV) uint64
}

var ccState sync.Map

func getCC(db *KV) *dbCC {
	v, ok := ccState.Load(db)
	if ok {
		return v.(*dbCC)
	}
	d := &dbCC{readers: make(map[uint64]uint64)}
	actual, _ := ccState.LoadOrStore(db, d)
	return actual.(*dbCC)
}

func (db *KV) RegisterFreeSeqProvider(fn func(*KV) uint64) {
	getCC(db).seqFn = fn
}

func (db *KV) BeginWrite() *Tx {
	getCC(db).wmu.Lock()
	tx := db.Begin()
	tx.release = func() { getCC(db).wmu.Unlock() }
	return tx
}

func (db *KV) BeginRead() *readGuard {
	cc := getCC(db)
	seq := uint64(0)
	if cc.seqFn != nil {
		seq = cc.seqFn(db)
	}
	cc.mu.Lock()
	cc.nextID++
	id := cc.nextID
	cc.readers[id] = seq
	cc.mu.Unlock()
	return &readGuard{db: db, seq: seq, id: id}
}

func (r *readGuard) End() {
	if r == nil || r.db == nil {
		return
	}
	if !atomic.CompareAndSwapUint32(&r.done, 0, 1) {
		return
	}
	cc := getCC(r.db)
	cc.mu.Lock()
	delete(cc.readers, r.id)
	cc.mu.Unlock()
}

func (db *KV) OldestActiveReaderSeq() uint64 {
	cc := getCC(db)
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if len(cc.readers) == 0 {
		return ^uint64(0)
	}
	var min uint64 = ^uint64(0)
	for _, s := range cc.readers {
		if s < min {
			min = s
		}
	}
	return min
}
