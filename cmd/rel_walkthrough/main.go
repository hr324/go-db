package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go-db/btree"
	"go-db/rel"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("kv_tables_%d.db", time.Now().UnixNano()))
	var kv btree.KV
	kv.Path = path
	must(kv.Open())
	defer kv.Close()

	users := rel.NewTable(&kv, "users")

	type Row struct {
		Name  string `json:"name"`
		Email string `json:"email"`
		Age   int    `json:"age"`
	}

	put := func(pk string, r Row) {
		b, _ := json.Marshal(r)
		must(users.Put([]byte(pk), b))
	}

	get := func(pk string) {
		v, ok := users.Get([]byte(pk))
		if !ok {
			fmt.Println("get", pk, "-> MISS")
			return
		}
		fmt.Println("get", pk, "->", string(v))
	}

	del := func(pk string) {
		ok, err := users.Del([]byte(pk))
		must(err)
		fmt.Println("del", pk, "->", ok)
	}

	put("001", Row{"Maya", "m@x.com", 21})
	put("004", Row{"Ali", "a@y.com", 19})
	put("120", Row{"Zoe", "z@z.com", 30})
	put("777", Row{"Lee", "l@w.com", 25})

	get("004")

	fmt.Println("scan all users:")
	users.Scan(func(pk, v []byte) bool {
		fmt.Println(string(pk), string(v))
		return true
	})

	fmt.Println("scan id in [100,700):")
	users.ScanRange([]byte("100"), []byte("700"), func(pk, v []byte) bool {
		fmt.Println(string(pk), string(v))
		return true
	})

	del("004")
	get("004")

	fmt.Println("scan all after delete:")
	users.Scan(func(pk, v []byte) bool {
		fmt.Println(string(pk), string(v))
		return true
	})
}
