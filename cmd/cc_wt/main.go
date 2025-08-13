package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go-db/btree"
)

func size(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return fi.Size()
}

func seed(kv *btree.KV, n int, vlen int) {
	tx := kv.BeginWrite()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		v := make([]byte, vlen)
		copy(v, []byte(fmt.Sprintf("seed-%d", i)))
		_ = tx.Set([]byte(k), v)
	}
	_ = tx.Commit()
}

func churn(kv *btree.KV, label string, start, count, vlen int, delEvery int) {
	tx := kv.BeginWrite()
	for i := start; i < start+count; i++ {
		k := fmt.Sprintf("k%06d", i)
		if delEvery > 0 && i%delEvery == 0 {
			_, _ = tx.Del([]byte(k))
			continue
		}
		doc := map[string]any{"k": k, "v": label, "t": time.Now().UnixNano()}
		b, _ := json.Marshal(doc)
		if vlen > len(b) {
			p := make([]byte, vlen)
			copy(p, b)
			b = p
		}
		_ = tx.Set([]byte(k), b)
	}
	_ = tx.Commit()
}

func main() {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("kv_cc_%d.db", time.Now().UnixNano()))
	var kv btree.KV
	kv.Path = path
	_ = kv.Open()
	defer kv.Close()

	seed(&kv, 4000, 64)

	fmt.Println("file size after seed:", size(path))

	r := kv.BeginRead()
	fmt.Println("snapshot reader started; seq:", kv.FreeTailSeq())

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		t0 := time.Now()
		churn(&kv, "writerA", 0, 4000, 96, 10)
		fmt.Println("writerA committed in", time.Since(t0))
	}()

	go func() {
		defer wg.Done()
		t0 := time.Now()
		churn(&kv, "writerB", 4000, 4000, 96, 0)
		fmt.Println("writerB committed in", time.Since(t0))
	}()

	time.Sleep(2 * time.Second)
	fmt.Println("file size while reader pinned:", size(path))
	time.Sleep(2 * time.Second)

	r.End()
	fmt.Println("snapshot reader ended")

	t0 := time.Now()
	churn(&kv, "writerC", 0, 4000, 72, 20)
	fmt.Println("writerC committed in", time.Since(t0))

	fmt.Println("final file size:", size(path))
}
