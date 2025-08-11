package btree

import (
	"encoding/binary"
	"fmt"
	"os"
)

type KV struct {
	Path string
	file *os.File
	tree BTree
	free FreeList
	page struct {
		flushed uint64
		nappend uint64
		updates map[uint64][]byte
	}
	cache  map[uint64][]byte
	failed bool
}

func (db *KV) Open() error {
	f, err := createFileSync(db.Path)
	if err != nil {
		return err
	}
	db.file = f
	fi, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	db.page.updates = make(map[uint64][]byte)
	db.cache = make(map[uint64][]byte)
	if err := readRoot(db, fi.Size()); err != nil {
		return err
	}
	db.tree.get = db.pageRead
	db.tree.new = db.pageAlloc
	db.tree.del = db.free.PushTail
	db.free.get = db.pageRead
	db.free.new = db.pageAppend
	db.free.set = db.pageWrite
	db.free.maxSeq = db.free.tailSeq
	return nil
}

func (db *KV) Close() error {
	if db.file == nil {
		return nil
	}
	err := db.file.Close()
	db.file = nil
	return err
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db)
	if err := db.tree.Insert(key, val); err != nil {
		return err
	}
	return updateOrRevert(db, meta)
}

func (db *KV) Del(key []byte) (bool, error) {
	meta := saveMeta(db)
	deleted, err := db.tree.Delete(key)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	return true, updateOrRevert(db, meta)
}

func (db *KV) pageRead(ptr uint64) []byte {
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}
	if p, ok := db.cache[ptr]; ok {
		return p
	}
	return db.pageReadFile(ptr)
}

func (db *KV) pageReadFile(ptr uint64) []byte {
	buf := make([]byte, BTREE_PAGE_SIZE)
	off := int64(ptr) * int64(BTREE_PAGE_SIZE)
	n, err := db.file.ReadAt(buf, off)
	if err != nil || n != BTREE_PAGE_SIZE {
		panic("bad read")
	}
	db.cache[ptr] = buf
	return buf
}

func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + db.page.nappend
	copyBuf := make([]byte, BTREE_PAGE_SIZE)
	copy(copyBuf, node[:BTREE_PAGE_SIZE])
	db.page.updates[ptr] = copyBuf
	db.page.nappend++
	return ptr
}

func (db *KV) pageAlloc(node []byte) uint64 {
	if ptr := db.free.PopHead(); ptr != 0 {
		copyBuf := make([]byte, BTREE_PAGE_SIZE)
		copy(copyBuf, node[:BTREE_PAGE_SIZE])
		db.page.updates[ptr] = copyBuf
		return ptr
	}
	return db.pageAppend(node)
}

func (db *KV) pageWrite(ptr uint64) []byte {
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}
	node := make([]byte, BTREE_PAGE_SIZE)
	copy(node, db.pageReadFile(ptr))
	db.page.updates[ptr] = node
	return node
}

func writePages(db *KV) error {
	for i := uint64(0); i < db.page.nappend; i++ {
		ptr := db.page.flushed + i
		pg := db.page.updates[ptr]
		off := int64(ptr) * int64(BTREE_PAGE_SIZE)
		n, err := db.file.WriteAt(pg, off)
		if err != nil {
			return err
		}
		if n != BTREE_PAGE_SIZE {
			return fmt.Errorf("short write")
		}
		db.cache[ptr] = append([]byte(nil), pg...)
	}
	for ptr, pg := range db.page.updates {
		if ptr < db.page.flushed {
			off := int64(ptr) * int64(BTREE_PAGE_SIZE)
			n, err := db.file.WriteAt(pg, off)
			if err != nil {
				return err
			}
			if n != BTREE_PAGE_SIZE {
				return fmt.Errorf("short write")
			}
			db.cache[ptr] = append([]byte(nil), pg...)
		}
	}
	db.page.flushed += db.page.nappend
	db.page.nappend = 0
	db.page.updates = make(map[uint64][]byte)
	return nil
}

const DB_SIG = "BuildYourOwnDB07"

func saveMeta(db *KV) []byte {
	var data [64]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	binary.LittleEndian.PutUint64(data[32:], db.free.headPage)
	binary.LittleEndian.PutUint64(data[40:], db.free.headSeq)
	binary.LittleEndian.PutUint64(data[48:], db.free.tailPage)
	binary.LittleEndian.PutUint64(data[56:], db.free.tailSeq)
	return data[:]
}

func loadMeta(db *KV, data []byte) {
	if string(data[:16]) != DB_SIG {
		return
	}
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
	db.free.headPage = binary.LittleEndian.Uint64(data[32:])
	db.free.headSeq = binary.LittleEndian.Uint64(data[40:])
	db.free.tailPage = binary.LittleEndian.Uint64(data[48:])
	db.free.tailSeq = binary.LittleEndian.Uint64(data[56:])
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 {
		db.page.flushed = 2
		db.free.headPage = 1
		db.free.tailPage = 1
		return nil
	}
	buf := make([]byte, 64)
	n, err := db.file.ReadAt(buf, 0)
	if err != nil || n != 64 {
		return fmt.Errorf("read meta")
	}
	loadMeta(db, buf)
	return nil
}

func updateRoot(db *KV) error {
	data := saveMeta(db)
	n, err := db.file.WriteAt(data, 0)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("short meta write")
	}
	return nil
}

func updateFile(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	if err := db.file.Sync(); err != nil {
		return err
	}
	if err := updateRoot(db); err != nil {
		return err
	}
	if err := db.file.Sync(); err != nil {
		return err
	}
	db.free.SetMaxSeq()
	return nil
}

func updateOrRevert(db *KV, meta []byte) error {
	if db.failed {
		if _, err := db.file.WriteAt(meta, 0); err != nil {
			return err
		}
		if err := db.file.Sync(); err != nil {
			return err
		}
		db.failed = false
	}
	err := updateFile(db)
	if err != nil {
		loadMeta(db, meta)
		db.page.updates = make(map[uint64][]byte)
		db.page.nappend = 0
		db.failed = true
	}
	return err
}

func createFileSync(file string) (*os.File, error) {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return f, nil
}
