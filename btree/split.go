package btree

func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2)
	nleft := old.nkeys() / 2
	leftBytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for leftBytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert(nleft >= 1)
	rightBytes := func() uint16 {
		return old.nbytes() - leftBytes() + 4
	}
	for rightBytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	assert(nleft < old.nkeys())
	nright := old.nkeys() - nleft
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right}
}
