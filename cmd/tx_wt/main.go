package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-db/btree"
	"go-db/rel"
)

type Row struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func emailIndex(row []byte) [][]byte {
	var r Row
	_ = json.Unmarshal(row, &r)
	return [][]byte{[]byte(strings.ToLower(r.Email))}
}

func ageIndex(row []byte) [][]byte {
	var r Row
	_ = json.Unmarshal(row, &r)
	s := fmt.Sprintf("%03d", r.Age)
	return [][]byte{[]byte(s)}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("kv_tx_%d.db", time.Now().UnixNano()))
	var kv btree.KV
	kv.Path = path
	must(kv.Open())
	defer kv.Close()

	users := rel.NewTable(&kv, "users")
	users.CreateIndex("email", emailIndex)
	users.CreateIndex("age", ageIndex)

	tx := kv.Begin()
	{
		b1, _ := json.Marshal(Row{"Maya", "m@x.com", 21})
		b2, _ := json.Marshal(Row{"Ali", "a@y.com", 19})
		b3, _ := json.Marshal(Row{"Zoe", "z@z.com", 30})
		must(users.PutTx(tx, []byte("001"), b1))
		must(users.PutTx(tx, []byte("004"), b2))
		must(users.PutTx(tx, []byte("120"), b3))
		_, _ = users.DelTx(tx, []byte("120"))
		b4, _ := json.Marshal(Row{"Ali", "ali@new.com", 19})
		must(users.PutTx(tx, []byte("004"), b4))
	}
	must(tx.Commit())

	fmt.Println("after tx1 commit:")
	users.Scan(func(pk, v []byte) bool {
		fmt.Println(string(pk), string(v))
		return true
	})
	fmt.Println("lookup by email ali@new.com:")
	users.IndexGet("email", []byte("ali@new.com"), func(pk []byte) bool {
		fmt.Println(string(pk))
		return true
	})

	tx2 := kv.Begin()
	{
		b1, _ := json.Marshal(Row{"Kai", "k@k.com", 22})
		must(users.PutTx(tx2, []byte("777"), b1))
		large := make([]byte, 4001)
		err := users.PutTx(tx2, []byte("999"), large)
		if err != nil {
			tx2.Rollback()
			fmt.Println("tx2 rolled back")
		} else {
			_ = tx2.Commit()
		}
	}

	fmt.Println("after tx2:")
	users.Scan(func(pk, v []byte) bool {
		fmt.Println(string(pk), string(v))
		return true
	})
}
