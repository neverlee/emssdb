package emssdb

import (
	"encoding/binary"
	"time"
)

func encodeExkvKey(key Bytes) (ret Bytes) {
	return encodeOneKey(DTEXKV, key)
}

func decodeExkvKey(slice Bytes) (ret Bytes) {
	return decodeOneKey(slice)
}

func encodeExkvValue(value Bytes, stamp uint64) (ret Bytes) {
	rb := make(Bytes, 8+len(value))
	binary.BigEndian.PutUint64(rb, stamp)
	copy(rb[8:], value)
	return rb
}

func decodeExkvValue(slice Bytes) (ret Bytes, stamp uint64) {
	return slice[8:], binary.BigEndian.Uint64(slice[:8])
}

func encodeExstampKey(key Bytes, stamp uint64) (ret Bytes) {
	rb := make([]byte, 9+len(key))
	rb[0] = DTEXSTAMP
	binary.BigEndian.PutUint64(rb[1:9], stamp)
	copy(rb[9:], key)
	return rb
}

func decodeExstampKey(slice Bytes) (ret Bytes, stamp uint64) {
	return decodeExkvValue(slice[1:])
}

func (db *DB) Eset(key, val Bytes, etime uint64) (err error) {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	writer := db.writer
	writer.Do()
	defer writer.Done()
	// readoption
	ekey := encodeExkvKey(key)
	eval := encodeExkvValue(val, etime)
	writer.Put(ekey, eval)
	xkey := encodeExstampKey(key, etime)
	writer.Put(xkey, nil)
	return writer.Commit()
}

func (db *DB) Edel(key Bytes) (err error) {
	writer := db.writer
	writer.Do()
	defer writer.Done()
	// readoption
	_, etime, _ := db.Eget(key)
	ekey := encodeExkvKey(key)
	writer.Delete(ekey)
	xkey := encodeExstampKey(key, etime)
	writer.Delete(xkey)
	return writer.Commit()
}

func (db *DB) EsetDelay(delay time.Duration) {
	db.expireDelay = delay
}

func (db *DB) Eget(key Bytes) (ret Bytes, stamp uint64, err error) {
	// readoption
	rkey := encodeExkvKey(key)
	slice, _ := db.db.Get(rkey, nil)
	if len(slice) < 8 {
		return nil, 0, nil
	}
	v, s := decodeExkvValue(slice)
	return v, s, nil
}

func (db *DB) Escan(start Bytes, end Bytes) (ret *EIterator) {
	key_start, key_end := encodeExkvKey(start), encodeExkvKey(end)
	if len(end) == 0 {
		key_end = encodeOneKey(DTEXKV+1, end)
	}
	return NewEIterator(db.Iterator(key_start, key_end))
}

func (db *DB) Erscan(start Bytes, end Bytes) (ret *EIterator) {
	key_start, key_end := encodeExkvKey(start), encodeExkvKey(end)
	if len(end) == 0 {
		key_end = encodeOneKey(DTEXKV+1, end)
	}
	return NewEIterator(db.RevIterator(key_start, key_end))
}

func (db *DB) Elist(start uint64, end uint64) (ret *XIterator) {
	if end < start {
		end = start + 1
	}
	key_start, key_end := encodeExstampKey(nil, start), encodeExstampKey(nil, end)
	return NewXIterator(db.Iterator(key_start, key_end))
}

func (db *DB) expireDaemon() {
	db.waitgroup.Add(1)

	for !db.end {
		now := time.Now().Unix()
		xit := db.Elist(0, uint64(now))
		for xit.Next() {
			db.Edel(xit.Key())
		}
		time.Sleep(db.expireDelay)
	}
	db.waitgroup.Done()
}
