package emssdb

import (
	"bytes"
	"github.com/syndtr/goleveldb/leveldb"
)

func encodeHsizeKey(name Bytes) (ret Bytes) {
	return encodeOneKey(DTHSIZE, name)
}

func decodeHsizeKey(slice Bytes) (ret Bytes) {
	return decodeOneKey(slice)
}

func encodeHashKey(name Bytes, key Bytes) (ret Bytes) {
	return encodeTwoKey(DTHASH, name, 0, key)
}

func decodeHashKey(slice Bytes) (name, key Bytes) {
	return decodeTwoKey(slice)
}

func (this *DB) Hget(name, key Bytes) (val Bytes, err error) {
	// readoption
	if verr := isVaildHashKey(name, key); verr != nil {
		return nil, verr
	}
	rkey := encodeHashKey(name, key)
	return this.db.Get(rkey, nil)
}

func (this *DB) Hset(name, key, val Bytes) (err error) {
	if verr := isVaildHashKey(name, key); verr != nil {
		return verr
	}
	writer := this.writer
	writer.Do()
	defer writer.Done()
	// readoption
	if st := this.hsetOne(name, key, val); st == StatSucChange {
		if err := this.hincrSize(name, 1); err != nil {
			return err
		}
	} else if st == StatSuccess {
	} else {
		return st
	}
	return writer.Commit()
}

func (this *DB) Hdel(name, key Bytes) (err error) {
	if verr := isVaildHashKey(name, key); verr != nil {
		return verr
	}
	writer := this.writer
	writer.Do()
	defer writer.Done()
	// readoption
	if st := this.hdelOne(name, key); st == StatSucChange {
		this.hincrSize(name, -1)
	}
	return writer.Commit()
}

func (this *DB) Hincr(name, key Bytes, by int64) (newval int64, err error) {
	if verr := isVaildHashKey(name, key); verr != nil {
		return 0, verr
	}
	writer := this.writer
	writer.Do()
	defer writer.Done()
	// readoption
	var ival int64
	if oldvar, oerr := this.Hget(name, key); oerr == leveldb.ErrNotFound {
		ival = by
		this.hincrSize(name, 1)
	} else if oerr == nil {
		ival = oldvar.GetInt64() + by
	} else {
		return 0, oerr
	}

	buf := NewByInt64(ival)

	if st := this.hsetOne(name, key, buf); st == StatSuccess || st == StatSucChange {
		return ival, writer.Commit()
	} else {
		return ival, st
	}
}

func (this *DB) Hsize(name Bytes) (ret int64, err error) {
	skey := encodeHsizeKey(name)
	// readoption
	ssize, serr := this.db.Get(skey, nil)
	return Bytes(ssize).GetInt64(), serr
}

func (this *DB) Hscan(name, start, end Bytes) (ret *HIterator) {
	key_start, key_end := encodeHashKey(name, start), encodeHashKey(name, end)
	if len(end) == 0 {
		key_end = encodeTwoKey(DTHASH, name, 1, nil)
	}
	return NewHIterator(this.Iterator(key_start, key_end))
}

func (this *DB) Hrscan(name, start, end Bytes) (ret *HIterator) {
	key_start, key_end := encodeHashKey(name, start), encodeHashKey(name, end)
	if len(end) == 0 {
		key_end = encodeTwoKey(DTHASH, name, 1, nil)
	}
	return NewHIterator(this.RevIterator(key_start, key_end))
}

func (this *DB) Hlist(sname, ename Bytes) (ret []Bytes) {
	start, end := encodeHsizeKey(sname), encodeHsizeKey(ename)
	if len(ename) == 0 {
		end = encodeOneKey(DTHSIZE+1, ename)
	}
	it := this.Iterator(start, end)
	list := make([]Bytes, 0)
	for it.Next() {
		ks := it.Key()
		list = append(list, ks[1:])
	}
	return list
}

func (this *DB) hsetOne(name, key, val Bytes) (ret Status) {
	writer := this.writer
	if dbval, hgerr := this.Hget(name, key); hgerr != nil {
		hkey := encodeHashKey(name, key)
		writer.Put(hkey, val)
		return StatSucChange
	} else {
		if bytes.Compare(dbval, val) != 0 {
			hkey := encodeHashKey(name, key)
			writer.Put(hkey, val)
		}
		return StatSuccess
	}
}

func (this *DB) hdelOne(name, key Bytes) (ret Status) {
	if len(key) == 0 || len(name) == 0 {
		return ErrEmptyKey
	}
	writer := this.writer
	if _, hgerr := this.Hget(name, key); hgerr == nil {
		hkey := encodeHashKey(name, key)
		writer.Delete(hkey)
		return StatSucChange
	} else {
		return StatNotFound
	}
}

func (this *DB) hincrSize(name Bytes, incr int64) (ret error) {
	writer := this.writer
	if isize, ierr := this.Hsize(name); ierr == nil || ierr == leveldb.ErrNotFound {
		isize += incr
		skey := encodeHsizeKey(name)
		if isize == 0 {
			writer.Delete(skey)
		} else {
			buf := NewByInt64(isize)
			writer.Put(skey, buf)
		}
		return nil
	} else {
		return ierr
	}
}
