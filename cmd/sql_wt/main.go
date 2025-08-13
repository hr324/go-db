package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-db/btree"
	"go-db/sqlmini"
)

func main() {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("kv_sql_%d.db", time.Now().UnixNano()))
	var kv btree.KV
	kv.Path = path
	_ = kv.Open()
	defer kv.Close()

	cat := sqlmini.NewCatalog(&kv)

	in := bufio.NewScanner(os.Stdin)
	fmt.Println("ready")
	for {
		fmt.Print("> ")
		if !in.Scan() {
			break
		}
		line := in.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		err := cat.ExecAll(line, func(s string) { fmt.Println(s) })
		if err != nil {
			fmt.Println("ERR:", err.Error())
		}
	}
}
