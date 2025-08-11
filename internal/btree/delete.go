package btree

import "bytes"

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	newNode := BNode(make([]byte, BTREE_PAGE_SIZE))
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF_TYPE:
		if idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
			leafDelete(newNode, node, idx)
			return newNode
		}
		return BNode{}
	case BNODE_NODE_TYPE:
		return nodeDelete(tree, node, idx, key)
	default:
		return BNode{}
	}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, BNode(tree.get(kptr)), key)
	if len(updated) == 0 {
		return BNode{}
	}
	tree.del(kptr)
	newNode := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(newNode, node, idx-1, tree.new(merged[:BTREE_PAGE_SIZE]), merged.getKey(0))
	case mergeDir > 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(newNode, node, idx, tree.new(merged[:BTREE_PAGE_SIZE]), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		newNode.setHeader(BNODE_NODE_TYPE, 0)
	case mergeDir == 0 && updated.nkeys() > 0:
		nodeReplaceKidN(tree, newNode, node, idx, updated)
	}
	return newNode
}
