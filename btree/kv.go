package btree

func (tree *BTree) Insert(key []byte, val []byte) error {
	if err := checkLimit(key, val); err != nil {
		return err
	}
	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF_TYPE, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root[:BTREE_PAGE_SIZE])
		return nil
	}
	node := treeInsert(tree, BNode(tree.get(tree.root)), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE_TYPE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, k := tree.new(knode[:BTREE_PAGE_SIZE]), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, k, nil)
		}
		tree.root = tree.new(root[:BTREE_PAGE_SIZE])
	} else {
		tree.root = tree.new(split[0][:BTREE_PAGE_SIZE])
	}
	return nil
}

func (tree *BTree) Delete(key []byte) (bool, error) {
	if tree.root == 0 {
		return false, nil
	}
	updated := treeDelete(tree, BNode(tree.get(tree.root)), key)
	if len(updated) == 0 {
		return false, nil
	}
	tree.del(tree.root)
	if updated.nkeys() == 0 {
		tree.root = 0
		return true, nil
	}
	tree.root = tree.new(updated[:BTREE_PAGE_SIZE])
	return true, nil
}
