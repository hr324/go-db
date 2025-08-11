package btree

import (
	"fmt"
	"strings"
)

type demoPager struct {
	data map[uint64][]byte
	next uint64
}

func newDemoPager() *demoPager            { return &demoPager{data: map[uint64][]byte{}, next: 1} }
func (p *demoPager) Get(id uint64) []byte { return p.data[id] }
func (p *demoPager) New(b []byte) uint64 {
	id := p.next
	p.next++
	p.data[id] = append([]byte(nil), b...)
	return id
}
func (p *demoPager) Del(id uint64) { delete(p.data, id) }

func Demo() string {
	var out strings.Builder

	// --- Single leaf: {"k1":"hi","k3":"hello"} ---
	leaf := BNode(make([]byte, BTREE_PAGE_SIZE))
	leaf.setHeader(BNODE_LEAF_TYPE, 2)
	nodeAppendKV(leaf, 0, 0, []byte("k1"), []byte("hi"))
	nodeAppendKV(leaf, 1, 0, []byte("k3"), []byte("hello"))

	fmt.Fprintln(&out, "=== Single Leaf ===")
	out.WriteString(leaf.Pretty())

	// --- Tiny tree: internal root -> two leaves ---
	p := newDemoPager()

	// leaf 1: {"a":"1","c":"3"}
	l1 := BNode(make([]byte, BTREE_PAGE_SIZE))
	l1.setHeader(BNODE_LEAF_TYPE, 2)
	nodeAppendKV(l1, 0, 0, []byte("a"), []byte("1"))
	nodeAppendKV(l1, 1, 0, []byte("c"), []byte("3"))
	id1 := p.New(l1[:BTREE_PAGE_SIZE])

	// leaf 2: {"m":"13","t":"20"}
	l2 := BNode(make([]byte, BTREE_PAGE_SIZE))
	l2.setHeader(BNODE_LEAF_TYPE, 2)
	nodeAppendKV(l2, 0, 0, []byte("m"), []byte("13"))
	nodeAppendKV(l2, 1, 0, []byte("t"), []byte("20"))
	id2 := p.New(l2[:BTREE_PAGE_SIZE])

	// internal root (tutorial’s variant): keys=first keys of children; ptrs=child pages
	root := BNode(make([]byte, BTREE_PAGE_SIZE))
	root.setHeader(BNODE_NODE_TYPE, 2)
	nodeAppendKV(root, 0, id1, l1.getKey(0), nil)
	nodeAppendKV(root, 1, id2, l2.getKey(0), nil)
	rootID := p.New(root[:BTREE_PAGE_SIZE])

	tree := &BTree{
		root: rootID,
		get:  func(id uint64) []byte { return p.Get(id) },
		new:  func(b []byte) uint64 { return p.New(b) },
		del:  func(id uint64) { p.Del(id) },
	}

	fmt.Fprintln(&out, "\n=== Whole Tree ===")
	out.WriteString(tree.Pretty())
	return out.String()
}
