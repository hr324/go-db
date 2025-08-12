package rel

import (
	"bytes"

	"go-db/btree"
)

type Table struct {
	Name string
	kv   *btree.KV
}

func NewTable(kv *btree.KV, name string) *Table {
	return &Table{kv: kv, Name: name}
}

func (t *Table) prefix() []byte {
	return []byte("t|" + t.Name + "|")
}

func (t *Table) key(pk []byte) []byte {
	p := t.prefix()
	k := make([]byte, 0, len(p)+len(pk))
	k = append(k, p...)
	k = append(k, pk...)
	return k
}

func (t *Table) Put(pk, row []byte) error {
	return t.kv.Set(t.key(pk), row)
}

func (t *Table) Get(pk []byte) ([]byte, bool) {
	return t.kv.Get(t.key(pk))
}

func (t *Table) Del(pk []byte) (bool, error) {
	return t.kv.Del(t.key(pk))
}

func (t *Table) Scan(fn func(pk, val []byte) bool) {
	start := t.prefix()
	end := append(append([]byte{}, start...), 0xFF)
	t.kv.Scan(start, end, func(k, v []byte) bool {
		pk := k[len(start):]
		return fn(pk, v)
	})
}

func (t *Table) ScanRange(startPK, endPK []byte, fn func(pk, val []byte) bool) {
	start := t.key(startPK)
	var end []byte
	if len(endPK) == 0 {
		tmp := append(t.prefix(), 0xFF)
		end = tmp
	} else {
		end = t.key(endPK)
	}
	t.kv.Scan(start, end, func(k, v []byte) bool {
		pk := k[len(t.prefix()):]
		if len(endPK) != 0 && bytes.Compare(pk, endPK) >= 0 {
			return false
		}
		return fn(pk, v)
	})
}
