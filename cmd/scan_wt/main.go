package main

import (
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

func main() {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("kv_scan_%d.db", time.Now().UnixNano()))
	var kv btree.KV
	kv.Path = path
	must(kv.Open())
	defer kv.Close()

	_ = os.Remove(path)

	must(kv.Open())

	put := func(k, v string) { must(kv.Set([]byte(k), []byte(v))) }
	put("a", "1")
	put("d", "1")
	put("k0000", "v")
	put("k0100", "v")
	put("m", "1")
	put("t", "1")
	put("x", "1")

	fmt.Println("scan [d, t):")
	kv.Scan([]byte("d"), []byte("t"), func(k, v []byte) bool {
		fmt.Printf("  %s=%s\n", k, v)
		return true
	})

	fmt.Println("scan prefix k0:")
	kv.Scan([]byte("k0"), []byte("k1"), func(k, v []byte) bool {
		fmt.Printf("  %s=%s\n", k, v)
		return true
	})
}
