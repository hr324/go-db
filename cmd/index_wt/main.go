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

type Row struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func emailIndex(row []byte) [][]byte {
	var r Row
	_ = json.Unmarshal(row, &r)
	return [][]byte{[]byte(r.Email)}
}

func ageIndex(row []byte) [][]byte {
	var r Row
	_ = json.Unmarshal(row, &r)
	s := fmt.Sprintf("%03d", r.Age)
	return [][]byte{[]byte(s)}
}

func main() {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("kv_idx_%d.db", time.Now().UnixNano()))
	var kv btree.KV
	kv.Path = path
	must(kv.Open())
	defer kv.Close()

	users := rel.NewTable(&kv, "users")
	users.CreateIndex("email", emailIndex)
	users.CreateIndex("age", ageIndex)

	put := func(pk string, r Row) { b, _ := json.Marshal(r); must(users.Put([]byte(pk), b)) }
	get := func(pk string) {
		v, ok := users.Get([]byte(pk))
		if ok {
			fmt.Println("get", pk, "->", string(v))
		} else {
			fmt.Println("get", pk, "-> MISS")
		}
	}
	del := func(pk string) { ok, err := users.Del([]byte(pk)); must(err); fmt.Println("del", pk, "->", ok) }

	put("001", Row{"Maya", "m@x.com", 21})
	put("004", Row{"Ali", "a@y.com", 19})
	put("120", Row{"Zoe", "z@z.com", 30})
	put("777", Row{"Lee", "l@w.com", 25})

	fmt.Println("by email = m@x.com")
	users.IndexGet("email", []byte("m@x.com"), func(pk []byte) bool {
		get(string(pk))
		return true
	})

	fmt.Println("by age in [020,030)")
	users.IndexScan("age", []byte("020"), []byte("030"), func(val, pk []byte) bool {
		get(string(pk))
		return true
	})

	put("004", Row{"Ali", "ali@new.com", 19})
	fmt.Println("after update email for 004")
	users.IndexGet("email", []byte("a@y.com"), func(pk []byte) bool {
		fmt.Println("should not see:", string(pk))
		return true
	})
	users.IndexGet("email", []byte("ali@new.com"), func(pk []byte) bool {
		get(string(pk))
		return true
	})

	del("120")
	fmt.Println("by age = 030 after delete")
	users.IndexGet("age", []byte("030"), func(pk []byte) bool {
		fmt.Println("should be empty")
		return true
	})
}
