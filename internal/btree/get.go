package btree

import "bytes"

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	node := BNode(tree.get(tree.root))
	for {
		idx := nodeLookupLE(node, key)
		if node.btype() == BNODE_LEAF_TYPE {
			if idx > 0 && bytes.Equal(node.getKey(idx), key) {
				return node.getVal(idx), true
			}
			return nil, false
		}
		ptr := node.getPtr(idx)
		node = BNode(tree.get(ptr))
	}
}
