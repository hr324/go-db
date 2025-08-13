package rel

import "go-db/btree"

func (t *Table) PutTx(tx *btree.Tx, pk, row []byte) error {
	old, ok := t.Get(pk)
	if ok {
		for _, ix := range t.idx {
			for _, v := range ix.fn(old) {
				if _, err := tx.Del(t.idxKey(ix.Name, v, pk)); err != nil {
					return err
				}
			}
		}
	}
	if err := tx.Set(t.key(pk), row); err != nil {
		return err
	}
	for _, ix := range t.idx {
		for _, v := range ix.fn(row) {
			if err := tx.Set(t.idxKey(ix.Name, v, pk), nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Table) DelTx(tx *btree.Tx, pk []byte) (bool, error) {
	old, ok := t.Get(pk)
	if !ok {
		return false, nil
	}
	for _, ix := range t.idx {
		for _, v := range ix.fn(old) {
			if _, err := tx.Del(t.idxKey(ix.Name, v, pk)); err != nil {
				return false, err
			}
		}
	}
	ok2, err := tx.Del(t.key(pk))
	if err != nil {
		return false, err
	}
	return ok2, nil
}
