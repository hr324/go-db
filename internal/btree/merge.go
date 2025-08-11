package btree

const HEADER = 4

func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF_TYPE, old.nkeys()-1)
	if idx > 0 {
		nodeAppendRange(new, old, 0, 0, idx)
	}
	if idx+1 <= old.nkeys() {
		nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
	}
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE_TYPE, old.nkeys()-1)
	if idx > 0 {
		nodeAppendRange(new, old, 0, 0, idx)
	}
	nodeAppendKV(new, idx, ptr, key, nil)
	if idx+2 <= old.nkeys() {
		nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
	}
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sib := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sib.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sib
		}
	}
	if idx+1 < node.nkeys() {
		sib := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sib.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sib
		}
	}
	return 0, BNode{}
}
