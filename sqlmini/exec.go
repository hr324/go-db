package sqlmini

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go-db/btree"
	"go-db/rel"
)

type Catalog struct {
	KV      *btree.KV
	tables  map[string]*rel.Table
	idxfld  map[string]map[string]string
	inTx    *btree.Tx
	inWrite bool
}

func NewCatalog(kv *btree.KV) *Catalog {
	return &Catalog{
		KV:     kv,
		tables: make(map[string]*rel.Table),
		idxfld: make(map[string]map[string]string),
	}
}

func (c *Catalog) getTable(name string) *rel.Table {
	if t, ok := c.tables[name]; ok {
		return t
	}
	t := rel.NewTable(c.KV, name)
	c.tables[name] = t
	return t
}

func encodeFieldVal(v any) ([]byte, bool) {
	switch x := v.(type) {
	case string:
		return []byte(strings.ToLower(x)), true
	case float64:
		return []byte(fmt.Sprintf("%020.0f", x)), true
	case int:
		return []byte(fmt.Sprintf("%020d", x)), true
	case int64:
		return []byte(fmt.Sprintf("%020d", x)), true
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return []byte(fmt.Sprintf("%020d", i)), true
		}
	}
	return nil, false
}

func jsonFieldIndex(field string) rel.KeyFunc {
	return func(row []byte) [][]byte {
		var m map[string]any
		if err := json.Unmarshal(row, &m); err != nil {
			return nil
		}
		v, ok := m[field]
		if !ok {
			return nil
		}
		b, ok := encodeFieldVal(v)
		if !ok {
			return nil
		}
		return [][]byte{b}
	}
}

func (c *Catalog) ExecAll(sql string, out func(string)) error {
	p := newParser(sql)
	stmts := p.parse()
	for _, s := range stmts {
		if err := c.execOne(s, out); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) execOne(s stmt, out func(string)) error {
	switch s.k {
	case sBegin:
		if c.inWrite {
			return errors.New("tx open")
		}
		tx := c.KV.BeginWrite()
		c.inTx = tx
		c.inWrite = true
		out("OK")
		return nil
	case sCommit:
		if !c.inWrite {
			return errors.New("no tx")
		}
		err := c.inTx.Commit()
		c.inTx = nil
		c.inWrite = false
		if err != nil {
			return err
		}
		out("OK")
		return nil
	case sRollback:
		if !c.inWrite {
			return errors.New("no tx")
		}
		c.inTx.Rollback()
		c.inTx = nil
		c.inWrite = false
		out("OK")
		return nil
	case sCreateTable:
		_ = c.getTable(s.tbl)
		out("OK")
		return nil
	case sCreateIndex:
		t := c.getTable(s.tbl)
		t.CreateIndex(s.idx, jsonFieldIndex(s.field))
		if _, ok := c.idxfld[s.tbl]; !ok {
			c.idxfld[s.tbl] = make(map[string]string)
		}
		c.idxfld[s.tbl][s.field] = s.idx
		tx := c.KV.BeginWrite()
		t.Scan(func(pk, row []byte) bool {
			_ = t.PutTx(tx, pk, row)
			return true
		})
		_ = tx.Commit()
		out("OK")
		return nil
	case sInsert:
		t := c.getTable(s.tbl)
		var raw json.RawMessage
		if json.Unmarshal([]byte(s.json), &raw) != nil {
			return errors.New("bad json")
		}
		if c.inWrite {
			return t.PutTx(c.inTx, []byte(s.pk), []byte(s.json))
		}
		tx := c.KV.BeginWrite()
		if err := t.PutTx(tx, []byte(s.pk), []byte(s.json)); err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	case sDelete:
		t := c.getTable(s.tbl)
		if c.inWrite {
			_, err := t.DelTx(c.inTx, []byte(s.pk))
			return err
		}
		tx := c.KV.BeginWrite()
		_, err := t.DelTx(tx, []byte(s.pk))
		if err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	case sSelect:
		t := c.getTable(s.tbl)
		switch s.where {
		case wNone:
			t.Scan(func(pk, row []byte) bool {
				out(fmt.Sprintf("%s %s", string(pk), string(row)))
				return true
			})
			return nil
		case wPkEq:
			if v, ok := t.Get([]byte(s.pk)); ok {
				out(fmt.Sprintf("%s %s", s.pk, string(v)))
			}
			return nil
		case wPkRange:
			start := []byte(s.lo)
			end := []byte(s.hi)
			if !s.loInc {
				start = append([]byte(s.lo), 0)
			}
			if s.hiInc {
				end = append([]byte(s.hi), 0)
			}
			t.ScanRange(start, end, func(pk, row []byte) bool {
				out(fmt.Sprintf("%s %s", string(pk), string(row)))
				return true
			})
			return nil
		case wFieldEq:
			idx := c.idxfld[s.tbl][s.wField]
			val := jsonFieldIndex(s.wField)([]byte(`{"` + s.wField + `":"` + s.lo + `"}`))
			if len(val) == 0 {
				return nil
			}
			t.IndexGet(idx, val[0], func(pk []byte) bool {
				if v, ok := t.Get(pk); ok {
					out(fmt.Sprintf("%s %s", string(pk), string(v)))
				}
				return true
			})
			return nil
		case wFieldRange:
			idx := c.idxfld[s.tbl][s.wField]
			lo := jsonFieldIndex(s.wField)([]byte(`{"` + s.wField + `":"` + s.lo + `"}`))
			hi := jsonFieldIndex(s.wField)([]byte(`{"` + s.wField + `":"` + s.hi + `"}`))
			if len(lo) == 0 || len(hi) == 0 {
				return nil
			}
			loKey := lo[0]
			hiKey := hi[0]
			if !s.loInc {
				loKey = append(loKey, 0)
			}
			if s.hiInc {
				hiKey = append(hiKey, 0)
			}
			t.IndexScan(idx, loKey, hiKey, func(_ []byte, pk []byte) bool {
				if v, ok := t.Get(pk); ok {
					out(fmt.Sprintf("%s %s", string(pk), string(v)))
				}
				return true
			})
			return nil
		}
	}
	return errors.New("stmt")
}
