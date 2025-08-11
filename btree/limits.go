package btree

import "errors"

func checkLimit(key []byte, val []byte) error {
	if len(key) > BTREE_MAX_KEY_SIZE {
		return errors.New("key too large")
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		return errors.New("value too large")
	}
	return nil
}
