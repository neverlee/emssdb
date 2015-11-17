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
	bst := make([]byte, 9+len(key))
	bst[0] = DTEXSTAMP
	encodeExkvValue(key, stamp)
	return bst
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
	ekey := encodeKvKey(key)
	writer.Delete(ekey)
	xkey := encodeExstampKey(key, etime)
	writer.Delete(xkey)
	return writer.Commit()
}

//func (this *DB) Eincr(key Bytes, by int64) (newval int64, err error) {
//	writer := this.writer
//	writer.Do()
//	defer writer.Done()
//	// readoption
//	rkey := encodeKvKey(key)
//	var ival int64
//	if oldvar, oerr := this.db.Get(rkey, nil); oerr == leveldb.ErrNotFound {
//		ival = by
//	} else if err == nil {
//		ival = Bytes(oldvar).GetInt64() + by
//	} else {
//		return 0, err
//	}
//	writer.Put(rkey, NewByInt64(ival))
//	return ival, writer.Commit()
//}

func (this *DB) EsetDelay(delay time.Duration) {
	this.expireDelay = delay
}

func (this *DB) Eget(key Bytes) (ret Bytes, stamp uint64, err error) {
	// readoption
	rkey := encodeKvKey(key)
	slice, _ := this.db.Get(rkey, nil)
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
	for !this.close {
		now := time.Now().Unix()
		xit := this.Elist(0, uint64(now))
		for xit.Next() {
			this.Edel(xit.Key())
		}
		time.Sleep(this.expireDelay)
	}
	this.waitgroup.Done()
}
