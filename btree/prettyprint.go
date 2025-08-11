package btree

import (
	"fmt"
	"strings"
)

func (n BNode) Pretty() string {
	var b strings.Builder
	tn := "unknown"
	switch n.btype() {
	case BNODE_LEAF_TYPE:
		tn = "leaf"
	case BNODE_NODE_TYPE:
		tn = "internal"
	}
	fmt.Fprintf(&b, "node type=%s nkeys=%d used=%d\n", tn, n.nkeys(), n.nbytes())
	base := 4 + 8*n.nkeys() + 2*n.nkeys()
	fmt.Fprintf(&b, "header=4 ptrs=%d offs=%d base=%d\n", 8*n.nkeys(), 2*n.nkeys(), base)
	for i := uint16(1); i <= n.nkeys(); i++ {
		fmt.Fprintf(&b, "off[%d]=%d\n", i, n.getOffset(i))
	}
	for i := uint16(0); i < n.nkeys(); i++ {
		k := showBytes(n.getKey(i))
		if n.btype() == BNODE_LEAF_TYPE {
			v := showBytes(n.getVal(i))
			fmt.Fprintf(&b, "[%d] key=%s val=%s\n", i, k, v)
		} else {
			ptr := n.getPtr(i)
			fmt.Fprintf(&b, "[%d] key=%s ptr=%d\n", i, k, ptr)
		}
	}
	return b.String()
}

func (t *BTree) Pretty() string {
	if t == nil || t.root == 0 {
		return "(empty)\n"
	}
	var b strings.Builder
	seen := map[uint64]bool{}
	t.prettyAt(t.root, 0, &b, seen)
	return b.String()
}

func (t *BTree) prettyAt(page uint64, depth int, b *strings.Builder, seen map[uint64]bool) {
	if seen[page] {
		fmt.Fprintf(b, "%s#%d (cycle)\n", strings.Repeat("  ", depth), page)
		return
	}
	seen[page] = true
	raw := t.get(page)
	n := BNode(raw)
	tn := "unknown"
	switch n.btype() {
	case BNODE_LEAF_TYPE:
		tn = "leaf"
	case BNODE_NODE_TYPE:
		tn = "internal"
	}
	indent := strings.Repeat("  ", depth)
	fmt.Fprintf(b, "%s#%d %s nkeys=%d used=%d\n", indent, page, tn, n.nkeys(), n.nbytes())
	for i := uint16(0); i < n.nkeys(); i++ {
		k := showBytes(n.getKey(i))
		if n.btype() == BNODE_LEAF_TYPE {
			v := showBytes(n.getVal(i))
			fmt.Fprintf(b, "%s  [%d] key=%s val=%s\n", indent, i, k, v)
		} else {
			ptr := n.getPtr(i)
			fmt.Fprintf(b, "%s  [%d] key=%s -> #%d\n", indent, i, k, ptr)
		}
	}
	if n.btype() == BNODE_NODE_TYPE {
		for i := uint16(0); i < n.nkeys(); i++ {
			t.prettyAt(n.getPtr(i), depth+1, b, seen)
		}
	}
}

func showBytes(p []byte) string {
	if isPrintable(p) {
		const lim = 64
		if len(p) > lim {
			return fmt.Sprintf("%q…(%d)", string(p[:lim]), len(p))
		}
		return fmt.Sprintf("%q", string(p))
	}
	const h = 32
	if len(p) > h {
		return fmt.Sprintf("%x…(%d)", p[:h], len(p))
	}
	return fmt.Sprintf("%x", p)
}

func isPrintable(p []byte) bool {
	for _, c := range p {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}
