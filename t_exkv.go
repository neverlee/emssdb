package emssdb

import (
	"encoding/binary"
	"fmt"
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

func (this *DB) Eset(key, val Bytes, etime uint64) (err error) {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	writer := this.writer
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

func (this *DB) Edel(key Bytes) (err error) {
	writer := this.writer
	writer.Do()
	defer writer.Done()
	// readoption
	_, etime, _ := this.Eget(key)
	ekey := encodeExkvKey(key)
	writer.Delete(ekey)
	xkey := encodeExstampKey(key, etime)
	writer.Delete(xkey)
	return writer.Commit()
}

func (this *DB) EsetDelay(delay time.Duration) {
	this.expireDelay = delay
}

func (this *DB) Eget(key Bytes) (ret Bytes, stamp uint64, err error) {
	// readoption
	rkey := encodeExkvKey(key)
	slice, _ := this.db.Get(rkey, nil)
	if len(slice) < 8 {
		return nil, 0, nil
	}
	v, s := decodeExkvValue(slice)
	return v, s, nil
}

func (this *DB) Escan(start Bytes, end Bytes) (ret *EIterator) {
	key_start, key_end := encodeExkvKey(start), encodeExkvKey(end)
	if len(end) == 0 {
		key_end = encodeOneKey(DTEXKV+1, end)
	}
	return NewEIterator(this.Iterator(key_start, key_end))
}

func (this *DB) Erscan(start Bytes, end Bytes) (ret *EIterator) {
	key_start, key_end := encodeExkvKey(start), encodeExkvKey(end)
	if len(end) == 0 {
		key_end = encodeOneKey(DTEXKV+1, end)
	}
	return NewEIterator(this.RevIterator(key_start, key_end))
}

func (this *DB) Elist(start uint64, end uint64) (ret *XIterator) {
	if end < start {
		end = start + 1
	}
	key_start, key_end := encodeExstampKey(nil, start), encodeExstampKey(nil, end)
	return NewXIterator(this.Iterator(key_start, key_end))
}

func (this *DB) expireDaemon() {
	this.waitgroup.Add(1)

	for !this.end {
		now := time.Now().Unix()
		xit := this.Elist(0, uint64(now))
		for xit.Next() {
			this.Edel(xit.Key())
		}
		time.Sleep(this.expireDelay)
	}
	this.waitgroup.Done()
}
