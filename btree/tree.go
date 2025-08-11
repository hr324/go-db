package btree

import "bytes"

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	newNode := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF_TYPE:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(newNode, node, idx, key, val)
		} else {
			leafInsert(newNode, node, idx+1, key, val)
		}
	case BNODE_NODE_TYPE:
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, BNode(tree.get(kptr)), key, val)
		nsplit, split := nodeSplit3(knode)
		tree.del(kptr)
		nodeReplaceKidN(tree, newNode, node, idx, split[:nsplit]...)
	}
	return newNode
}

func nodeReplaceKidN(tree *BTree, newNode BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	newNode.setHeader(BNODE_NODE_TYPE, old.nkeys()+inc-1)
	nodeAppendRange(newNode, old, 0, 0, idx)
	for i, nd := range kids {
		ptr := tree.new(nd)
		nodeAppendKV(newNode, idx+uint16(i), ptr, nd.getKey(0), nil)
	}
	nodeAppendRange(newNode, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}
