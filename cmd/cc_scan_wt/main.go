package main

import (
	"fmt"
	"math/rand"
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

func seed(kv *btree.KV, n int, vlen int) {
	tx := kv.BeginWrite()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		v := make([]byte, vlen)
		copy(v, []byte("x"))
		_ = tx.Set([]byte(k), v)
	}
	_ = tx.Commit()
}

func scanCount(kv *btree.KV) int {
	cnt := 0
	kv.Scan([]byte("k000000"), []byte("k999999"), func(k, v []byte) bool {
		cnt++
		return true
	})
	return cnt
}

func churn(kv *btree.KV, start, count int, delEvery int) {
	tx := kv.BeginWrite()
	for i := start; i < start+count; i++ {
		k := fmt.Sprintf("k%06d", rand.Intn(start+count))
		if delEvery > 0 && i%delEvery == 0 {
			_, _ = tx.Del([]byte(k))
		} else {
			_ = tx.Set([]byte(k), []byte("y"))
		}
	}
	_ = tx.Commit()
}

func main() {
	rand.Seed(time.Now().UnixNano())
	path := filepath.Join(os.TempDir(), fmt.Sprintf("kv_ccscan_%d.db", time.Now().UnixNano()))
	var kv btree.KV
	kv.Path = path
	must(kv.Open())
	defer kv.Close()

	seed(&kv, 20000, 32)

	r := kv.BeginRead()
	done := make(chan struct{})
	go func() {
		t0 := time.Now()
		churn(&kv, 0, 30000, 7)
		fmt.Println("writer committed in", time.Since(t0))
		close(done)
	}()

	t0 := time.Now()
	c1 := scanCount(&kv)
	fmt.Println("snapshot scan count:", c1, "elapsed:", time.Since(t0))
	r.End()

	<-done
	c2 := scanCount(&kv)
	fmt.Println("post-commit scan count:", c2)
}
