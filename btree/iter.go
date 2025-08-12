package btree

import "bytes"

type iterFrame struct {
	ptr uint64
	idx int
}

type Iter struct {
	tree  *BTree
	stack []iterFrame
	leaf  BNode
	idx   int
	end   []byte
	ok    bool
}

func NewIter(t *BTree) *Iter {
	return &Iter{tree: t}
}

func (it *Iter) SeekGE(start []byte, end []byte) bool {
	it.stack = it.stack[:0]
	it.end = end
	if it.tree.root == 0 {
		it.ok = false
		return false
	}
	ptr := it.tree.root
	for {
		n := BNode(it.tree.get(ptr))
		if n.btype() == BNODE_LEAF_TYPE {
			idx := int(nodeLookupLE(n, start))
			if idx > 0 && bytes.Compare(n.getKey(uint16(idx)), start) >= 0 {
				it.idx = idx
			} else {
				it.idx = idx + 1
			}
			if it.idx >= int(n.nkeys()) {
				it.ok = it.advance()
				return it.ok
			}
			it.leaf = n
			it.ok = true
			if it.end != nil && bytes.Compare(it.leaf.getKey(uint16(it.idx)), it.end) >= 0 {
				it.ok = false
			}
			return it.ok
		}
		idx := int(nodeLookupLE(n, start))
		it.stack = append(it.stack, iterFrame{ptr: ptr, idx: idx})
		ptr = n.getPtr(uint16(idx))
	}
}

func (it *Iter) Key() []byte {
	if !it.ok {
		return nil
	}
	return it.leaf.getKey(uint16(it.idx))
}

func (it *Iter) Val() []byte {
	if !it.ok {
		return nil
	}
	return it.leaf.getVal(uint16(it.idx))
}

func (it *Iter) Valid() bool {
	return it.ok
}

func (it *Iter) Next() bool {
	if !it.ok {
		return false
	}
	it.idx++
	if it.idx < int(it.leaf.nkeys()) {
		if it.end != nil && bytes.Compare(it.leaf.getKey(uint16(it.idx)), it.end) >= 0 {
			it.ok = false
			return false
		}
		return true
	}
	it.ok = it.advance()
	return it.ok
}

func (it *Iter) advance() bool {
	for {
		if len(it.stack) == 0 {
			return false
		}
		top := it.stack[len(it.stack)-1]
		parent := BNode(it.tree.get(top.ptr))
		i := top.idx + 1
		if i < int(parent.nkeys()) {
			it.stack[len(it.stack)-1].idx = i
			ptr := parent.getPtr(uint16(i))
			for {
				n := BNode(it.tree.get(ptr))
				if n.btype() == BNODE_LEAF_TYPE {
					it.leaf = n
					it.idx = 1
					if it.idx >= int(n.nkeys()) {
						break
					}
					if it.end != nil && bytes.Compare(it.leaf.getKey(uint16(it.idx)), it.end) >= 0 {
						return false
					}
					return true
				}
				it.stack = append(it.stack, iterFrame{ptr: ptr, idx: 0})
				ptr = n.getPtr(0)
			}
		} else {
			it.stack = it.stack[:len(it.stack)-1]
		}
	}
}
