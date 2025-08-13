package btree

import (
	"errors"
	"os"
)

var ErrTxClosed = errors.New("tx closed")

type Tx struct {
	db     *KV
	meta   []byte
	closed bool
}

func (db *KV) ensureInit() {
	if db.Path == "" {
		return
	}
	st, err := os.Stat(db.Path)
	if err != nil {
		return
	}
	if st.Size() >= int64(2*BTREE_PAGE_SIZE) {
		return
	}
	f, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return
	}
	defer f.Close()
	buf := make([]byte, BTREE_PAGE_SIZE)
	_, _ = f.WriteAt(buf, int64(BTREE_PAGE_SIZE))
	_ = f.Sync()
}

func (db *KV) Begin() *Tx {
	db.ensureInit()
	return &Tx{db: db, meta: saveMeta(db)}
}

func (tx *Tx) Set(key []byte, val []byte) error {
	if tx.closed {
		return ErrTxClosed
	}
	return tx.db.tree.Insert(key, val)
}

func (tx *Tx) Del(key []byte) (bool, error) {
	if tx.closed {
		return false, ErrTxClosed
	}
	return tx.db.tree.Delete(key)
}

func (tx *Tx) Commit() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	return updateOrRevert(tx.db, tx.meta)
}

func (tx *Tx) Rollback() {
	if tx.closed {
		return
	}
	loadMeta(tx.db, tx.meta)
	tx.db.page.updates = make(map[uint64][]byte)
	tx.db.page.nappend = 0
	tx.closed = true
}

func (db *KV) Do(fn func(*Tx) error) error {
	tx := db.Begin()
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
