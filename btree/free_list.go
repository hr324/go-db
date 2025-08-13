package btree

import "encoding/binary"

type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

func (node LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(node[:8])
}
func (node LNode) setNext(next uint64) {
	binary.LittleEndian.PutUint64(node[:8], next)
}
func (node LNode) getPtr(idx int) uint64 {
	o := FREE_LIST_HEADER + idx*8
	return binary.LittleEndian.Uint64(node[o : o+8])
}
func (node LNode) setPtr(idx int, ptr uint64) {
	o := FREE_LIST_HEADER + idx*8
	binary.LittleEndian.PutUint64(node[o:o+8], ptr)
}

type FreeList struct {
	get      func(uint64) []byte
	new      func([]byte) uint64
	set      func(uint64) []byte
	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64
	maxSeq   uint64
}

func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

func flPop(fl *FreeList) (uint64, uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0
	}
	node := LNode(fl.get(fl.headPage))
	ptr := node.getPtr(seq2idx(fl.headSeq))
	fl.headSeq++
	if seq2idx(fl.headSeq) == 0 {
		head := fl.headPage
		fl.headPage = node.getNext()
		return ptr, head
	}
	return ptr, 0
}

func (fl *FreeList) PopHead() uint64 {
	ptr, head := flPop(fl)
	if head != 0 {
		fl.PushTail(head)
	}
	return ptr
}

func (fl *FreeList) PushTail(ptr uint64) {
	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	if seq2idx(fl.tailSeq) == 0 {
		next, head := flPop(fl)
		if next == 0 {
			next = fl.new(make([]byte, BTREE_PAGE_SIZE))
		}
		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

func (fl *FreeList) PopHeadLe(max uint64) uint64 {
	old := fl.maxSeq
	if max < old {
		fl.maxSeq = max
	}
	ptr := fl.PopHead()
	fl.maxSeq = old
	return ptr
}
