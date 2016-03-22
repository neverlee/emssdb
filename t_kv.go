package emssdb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

func encodeKvKey(key Bytes) (ret Bytes) {
	return encodeOneKey(DTKV, key)
}

func decodeKvKey(slice Bytes) (ret Bytes) {
	return decodeOneKey(slice)
}

func (db *DB) MultiSet(keys []Bytes, vals []Bytes) (err error) {
	writer := db.writer
	writer.Do()
	defer writer.Done()
	// readoption
	for i := 0; i < len(keys) && i <= len(vals); i++ {
		rkey := encodeKvKey(keys[i])
		writer.Put(rkey, vals[i])
	}
	return writer.Commit()
}

func (db *DB) MultiDelete(keys []Bytes) (err error) {
	writer := db.writer
	writer.Do()
	defer writer.Done()
	// readoption
	for _, key := range keys {
		rkey := encodeKvKey(key)
		writer.Delete(rkey)
	}
	return writer.Commit()
}

func (db *DB) Set(key Bytes, val Bytes) (err error) {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	writer := db.writer
	writer.Do()
	defer writer.Done()
	// readoption
	rkey := encodeKvKey(key)
	writer.Put(rkey, val)
	return writer.Commit()
}

func (db *DB) Del(key Bytes) (err error) {
	writer := db.writer
	writer.Do()
	defer writer.Done()
	// readoption
	rkey := encodeKvKey(key)
	writer.Delete(rkey)
	return writer.Commit()
}

func (db *DB) Incr(key Bytes, by int64) (newval int64, err error) {
	writer := db.writer
	writer.Do()
	defer writer.Done()
	// readoption
	rkey := encodeKvKey(key)
	var ival int64
	if oldvar, oerr := db.db.Get(rkey, nil); oerr == leveldb.ErrNotFound {
		ival = by
	} else if err == nil {
		ival = Bytes(oldvar).GetInt64() + by
	} else {
		return 0, err
	}
	writer.Put(rkey, NewByInt64(ival))
	return ival, writer.Commit()
}

func (db *DB) Get(key Bytes) (ret Bytes, err error) {
	// readoption
	rkey := encodeKvKey(key)
	return db.db.Get(rkey, nil)
}

func (db *DB) Scan(start Bytes, end Bytes) (ret *KIterator) {
	key_start, key_end := encodeKvKey(start), encodeKvKey(end)
	if len(end) == 0 {
		key_end = encodeOneKey(DTKV+1, end)
	}
	return NewKIterator(db.Iterator(key_start, key_end))
}

func (db *DB) Rscan(start Bytes, end Bytes) (ret *KIterator) {
	key_start, key_end := encodeKvKey(start), encodeKvKey(end)
	if len(end) == 0 {
		key_end = encodeOneKey(DTKV+1, end)
	}
	return NewKIterator(db.RevIterator(key_start, key_end))
}
