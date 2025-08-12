package btree

type ScanFn func(k, v []byte) bool

func (db *KV) Scan(start, end []byte, fn ScanFn) {
	it := NewIter(&db.tree)
	if !it.SeekGE(start, end) {
		return
	}
	for it.Valid() {
		if !fn(it.Key(), it.Val()) {
			return
		}
		if !it.Next() {
			return
		}
	}
}
