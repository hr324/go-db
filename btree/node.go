package btree

import "encoding/binary"

type BNode []byte

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) setOffset(idx uint16, val uint16) {
	if idx == 0 {
		return
	}
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	binary.LittleEndian.PutUint16(node[pos:], val)
}

func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}
