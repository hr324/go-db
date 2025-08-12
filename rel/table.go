package rel

import (
	"bytes"

	"go-db/btree"
)

type Table struct {
	Name string
	kv   *btree.KV
	idx  map[string]*Index
}

func NewTable(kv *btree.KV, name string) *Table {
	return &Table{kv: kv, Name: name, idx: make(map[string]*Index)}
}

func (t *Table) CreateIndex(name string, fn KeyFunc) {
	t.idx[name] = &Index{Table: t.Name, Name: name, fn: fn}
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

func (t *Table) idxPrefix(name string) []byte {
	return []byte("i|" + t.Name + "|" + name + "|")
}

func (t *Table) idxKey(name string, val, pk []byte) []byte {
	p := t.idxPrefix(name)
	k := make([]byte, 0, len(p)+len(val)+1+len(pk))
	k = append(k, p...)
	k = append(k, val...)
	k = append(k, '|')
	k = append(k, pk...)
	return k
}

func (t *Table) Put(pk, row []byte) error {
	old, ok := t.Get(pk)
	if ok {
		for _, ix := range t.idx {
			for _, v := range ix.fn(old) {
				_, _ = t.kv.Del(t.idxKey(ix.Name, v, pk))
			}
		}
	}
	if err := t.kv.Set(t.key(pk), row); err != nil {
		return err
	}
	for _, ix := range t.idx {
		for _, v := range ix.fn(row) {
			if err := t.kv.Set(t.idxKey(ix.Name, v, pk), nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Table) Get(pk []byte) ([]byte, bool) {
	return t.kv.Get(t.key(pk))
}

func (t *Table) Del(pk []byte) (bool, error) {
	old, ok := t.Get(pk)
	if ok {
		for _, ix := range t.idx {
			for _, v := range ix.fn(old) {
				_, _ = t.kv.Del(t.idxKey(ix.Name, v, pk))
			}
		}
	}
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

func (t *Table) IndexGet(name string, val []byte, fn func(pk []byte) bool) {
	p := append(t.idxPrefix(name), val...)
	p = append(p, '|')
	end := append(append([]byte{}, p...), 0xFF)
	t.kv.Scan(p, end, func(k, v []byte) bool {
		pk := k[len(p):]
		return fn(pk)
	})
}

func (t *Table) IndexScan(name string, startVal, endVal []byte, fn func(val, pk []byte) bool) {
	start := append(t.idxPrefix(name), startVal...)
	var end []byte
	if endVal == nil {
		end = append(t.idxPrefix(name), 0xFF)
	} else {
		end = append(t.idxPrefix(name), endVal...)
	}
	t.kv.Scan(start, end, func(k, v []byte) bool {
		rest := k[len(t.idxPrefix(name)):]
		i := bytes.LastIndexByte(rest, '|')
		if i < 0 {
			return false
		}
		val := rest[:i]
		pk := rest[i+1:]
		return fn(val, pk)
	})
}
