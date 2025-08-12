package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go-db/btree"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type meta struct {
	root     uint64
	pageUsed uint64
	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64
}

func readMeta(path string) (meta, error) {
	f, err := os.Open(path)
	if err != nil {
		return meta{}, err
	}
	defer f.Close()
	buf := make([]byte, 64)
	_, err = f.ReadAt(buf, 0)
	if err != nil {
		return meta{}, err
	}
	return meta{
		root:     binary.LittleEndian.Uint64(buf[16:24]),
		pageUsed: binary.LittleEndian.Uint64(buf[24:32]),
		headPage: binary.LittleEndian.Uint64(buf[32:40]),
		headSeq:  binary.LittleEndian.Uint64(buf[40:48]),
		tailPage: binary.LittleEndian.Uint64(buf[48:56]),
		tailSeq:  binary.LittleEndian.Uint64(buf[56:64]),
	}, nil
}

func readPage(path string, ptr uint64) []byte {
	f, err := os.Open(path)
	must(err)
	defer f.Close()
	buf := make([]byte, btree.BTREE_PAGE_SIZE)
	off := int64(ptr) * int64(btree.BTREE_PAGE_SIZE)
	_, err = f.ReadAt(buf, off)
	must(err)
	return buf
}

func seq2idx(seq uint64) int {
	return int(seq % btree.FREE_LIST_CAP)
}

func collectFreeList(path string, m meta, limit int) []uint64 {
	if m.headPage == 0 || m.tailPage == 0 {
		return nil
	}
	var out []uint64
	page := m.headPage
	seq := m.headSeq
	for len(out) < limit && (page != m.tailPage || seq2idx(seq) != seq2idx(m.tailSeq)) {
		node := readPage(path, page)
		idx := seq2idx(seq)
		o := btree.FREE_LIST_HEADER + idx*8
		ptr := binary.LittleEndian.Uint64(node[o : o+8])
		if ptr != 0 {
			out = append(out, ptr)
		}
		seq++
		if seq2idx(seq) == 0 {
			page = binary.LittleEndian.Uint64(node[:8])
			if page == 0 {
				break
			}
		}
	}
	return out
}

func printSnapshot(title string, kv *btree.KV, keys [][]byte) {
	fmt.Println("=== ", title, " ===")
	fi, err := os.Stat(kv.Path)
	if err == nil && fi.Size() >= 64 {
		m, err := readMeta(kv.Path)
		if err == nil {
			fmt.Printf("meta root=%d page_used=%d fl_head=%d@%d fl_tail=%d@%d\n", m.root, m.pageUsed, m.headPage, m.headSeq, m.tailPage, m.tailSeq)
			fl := collectFreeList(kv.Path, m, 24)
			fmt.Printf("free_list(%d) %v\n", len(fl), fl)
		} else {
			fmt.Println("meta unreadable")
		}
	} else {
		fmt.Println("meta not written yet")
	}
	for _, k := range keys {
		v, ok := kv.Get(k)
		if ok {
			fmt.Printf("get %q -> %q\n", string(k), string(v))
		} else {
			fmt.Printf("get %q -> MISS\n", string(k))
		}
	}
	fmt.Println()
}

func set(kv *btree.KV, k, v string) {
	err := kv.Set([]byte(k), []byte(v))
	must(err)
}

func del(kv *btree.KV, k string) {
	_, err := kv.Del([]byte(k))
	must(err)
}

func main() {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("kv_free_%d.db", time.Now().UnixNano()))
	_ = os.Remove(path)

	var kv btree.KV
	kv.Path = path
	must(kv.Open())
	defer kv.Close()

	printSnapshot("open(empty)", &kv, [][]byte{[]byte("a"), []byte("m")})

	set(&kv, "m", "1")
	printSnapshot("after set m:1", &kv, [][]byte{[]byte("m")})

	set(&kv, "d", "1")
	printSnapshot("after set d:1", &kv, [][]byte{[]byte("d"), []byte("m")})

	set(&kv, "x", "1")
	printSnapshot("after set x:1", &kv, [][]byte{[]byte("d"), []byte("m"), []byte("x")})

	set(&kv, "a", "1")
	set(&kv, "b", "1")
	set(&kv, "c", "1")
	printSnapshot("after set a,b,c", &kv, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("m"), []byte("x")})

	for i := 0; i < 240; i++ {
		k := fmt.Sprintf("k%04d", i)
		set(&kv, k, "v")
	}
	printSnapshot("after bulk insert (split likely)", &kv, [][]byte{[]byte("a"), []byte("d"), []byte("k000"), []byte("k239"), []byte("m"), []byte("x")})

	for i := 0; i < 140; i++ {
		k := fmt.Sprintf("k%04d", i)
		del(&kv, k)
	}
	printSnapshot("after bulk delete (merges likely)", &kv, [][]byte{[]byte("k000"), []byte("k139"), []byte("k140"), []byte("k239"), []byte("m")})

	del(&kv, "b")
	printSnapshot("after del b", &kv, [][]byte{[]byte("a"), []byte("b"), []byte("c")})

	set(&kv, "k250", "2")
	printSnapshot("after set k250:2 (should reuse)", &kv, [][]byte{[]byte("k250")})
}
