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
	page struct {
		flushed uint64
		temp    [][]byte
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
	if err := readRoot(db, fi.Size()); err != nil {
		return err
	}
	db.cache = make(map[uint64][]byte)
	db.tree.get = db.pageRead
	db.tree.new = db.pageAppend
	db.tree.del = func(uint64) {}
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
	if p, ok := db.cache[ptr]; ok {
		return p
	}
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
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node[:BTREE_PAGE_SIZE])
	return ptr
}

func writePages(db *KV) error {
	base := int64(db.page.flushed) * int64(BTREE_PAGE_SIZE)
	for i, pg := range db.page.temp {
		off := base + int64(i)*int64(BTREE_PAGE_SIZE)
		n, err := db.file.WriteAt(pg, off)
		if err != nil {
			return err
		}
		if n != BTREE_PAGE_SIZE {
			return fmt.Errorf("short write")
		}
		db.cache[db.page.flushed+uint64(i)] = append([]byte(nil), pg...)
	}
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

const DB_SIG = "BuildYourOwnDB06"

func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func loadMeta(db *KV, data []byte) {
	if string(data[:16]) != DB_SIG {
		return
	}
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 {
		db.page.flushed = 1
		return nil
	}
	buf := make([]byte, 32)
	n, err := db.file.ReadAt(buf, 0)
	if err != nil || n != 32 {
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
	return db.file.Sync()
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
		db.page.temp = db.page.temp[:0]
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
