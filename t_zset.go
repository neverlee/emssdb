package emssdb

import (
	"encoding/binary"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	sSDB_SCORE_MIN = 0x8000000000000000
	sSDB_SCORE_MAX = 0x7FFFffffFFFFffff
	zSET_MIDLEINT  = 0x8000000000000000
)

func enInt(i int64) uint64 {
	return uint64(i) + zSET_MIDLEINT
}

func deInt(i uint64) int64 {
	return int64(i - zSET_MIDLEINT)
}

func encodeZsizeKey(name Bytes) (ret Bytes) {
	return encodeOneKey(DTZSIZE, name)
}

func decodeZsizeKey(slice Bytes) (ret Bytes) {
	return decodeOneKey(slice)
}

// [DTZSET][len(name)][name][0][key]
func encodeZsetKey(name Bytes, key Bytes) (ret Bytes) {
	return encodeTwoKey(DTZSET, name, 0, key)
}

func decodeZsetKey(slice Bytes) (name, key Bytes) {
	return decodeTwoKey(slice)
}

// [DTZSCORE][len(name)][name][0][score][key]
func encodeZscoreKey(name Bytes, key Bytes, score int64) (ret Bytes) {
	length := 1 + 1 + len(name) + 1 + 8 + len(key)
	buf := make(Bytes, length)
	buf[0] = DTZSCORE
	buf[1] = byte(len(name))
	p := buf[2:]
	copy(p, name)
	p = p[len(name):]
	binary.BigEndian.PutUint64(p[1:], enInt(score))
	p = p[9:]
	copy(p, key)
	return buf
}

func decodeZscoreKey(slice Bytes) (name Bytes, key Bytes, score int64) {
	p := slice[2:]
	gname := p[:slice[1]]
	p = p[len(gname)+1:]
	gscore := deInt(Bytes(p[:8]).GetUInt64())
	gkey := p[9:]
	return gname, gkey, gscore
}

func (this *DB) Zget(name, key Bytes) (score int64, err error) {
	// readoption
	rkey := encodeZsetKey(name, key)
	val, verr := this.db.Get(rkey, nil)
	return Bytes(val).GetInt64(), verr
}

func (this *DB) Zset(name, key Bytes, score int64) (err error) {
	writer := this.writer
	writer.Do()
	defer writer.Done()
	// readoption
	if st := this.zsetOne(name, key, score); st == StatSucChange {
		if err := this.zincrSize(name, 1); err != nil {
			return err
		}
	} else if st == StatSuccess {
	} else {
		return st
	}
	return writer.Commit()
}

func (this *DB) Zdel(name, key Bytes) (err error) {
	writer := this.writer
	writer.Do()
	defer writer.Done()
	// readoption
	if st := this.zdelOne(name, key); st == StatSucChange {
		this.zincrSize(name, -1)
	}
	return writer.Commit()
}

func (this *DB) Zincr(name Bytes, key Bytes, by int64) (newval int64, err error) {
	writer := this.writer
	writer.Do()
	defer writer.Done()
	// readoption
	var ival int64
	if oldvar, oerr := this.Zget(name, key); oerr == leveldb.ErrNotFound {
		ival = by
		this.hincrSize(name, 1)
	} else if oerr == nil {
		ival = oldvar + by
	} else {
		return 0, oerr
	}

	if st := this.zsetOne(name, key, ival); st == StatSuccess || st == StatSucChange {
		return ival, writer.Commit()
	} else {
		return ival, st
	}
}

func (this *DB) Zsize(name Bytes) (ret int64, err error) {
	skey := encodeZsizeKey(name)
	// readoption
	ssize, err := this.db.Get(skey, nil)
	return Bytes(ssize).GetInt64(), err
}

func (this *DB) Zscan(name Bytes, start, end int64) (ret *ZIterator) {
	key_start, key_end := encodeZscoreKey(name, nil, start), encodeZscoreKey(name, nil, end)
	return NewZIterator(this.Iterator(key_start, key_end))
}

func (this *DB) Zrscan(name Bytes, start, end int64) (ret *ZIterator) {
	key_start, key_end := encodeZscoreKey(name, nil, start), encodeZscoreKey(name, nil, end)
	return NewZIterator(this.RevIterator(key_start, key_end))
}

func (this *DB) Zlist(sname, ename Bytes) (ret []Bytes) {
	start, end := encodeZsizeKey(sname), encodeZsizeKey(ename)
	if len(ename) == 0 {
		end = encodeOneKey(DTZSIZE+1, nil)
	}
	it := this.Iterator(start, end)
	list := make([]Bytes, 0)
	for it.Next() {
		ks := it.Key()
		ks = ks[1:]
		list = append(list, ks)
	}
	return list
}

func (this *DB) zsetOne(name, key Bytes, score int64) (ret Status) {
	if verr := isVaildHashKey(name, key); verr != nil {
		return verr
	}
	writer := this.writer
	gosc, zgerr := this.Zget(name, key)
	if zgerr == nil {
		writer.Delete(encodeZscoreKey(name, key, gosc))
		ret = StatSuccess
	} else {
		ret = StatSucChange
	}
	writer.Put(encodeZscoreKey(name, key, score), nil)
	writer.Put(encodeZsetKey(name, key), NewByInt64(score))
	return
}

func (this *DB) zdelOne(name, key Bytes) (ret Status) {
	if verr := isVaildHashKey(name, key); verr != nil {
		return verr
	}
	writer := this.writer
	if gosc, zgerr := this.Zget(name, key); zgerr == nil {
		writer.Delete(encodeZsetKey(name, key))
		writer.Delete(encodeZscoreKey(name, key, gosc))
		return StatSucChange
	} else {
		return StatNotFound
	}
}

func (this *DB) zincrSize(name Bytes, incr int64) (ret error) {
	writer := this.writer
	if isize, ierr := this.Zsize(name); ierr == nil || ierr == leveldb.ErrNotFound {
		isize += incr
		skey := encodeZsizeKey(name)
		if isize == 0 {
			writer.Delete(skey)
		} else {
			writer.Put(skey, NewByInt64(isize))
		}
		return nil
	} else {
		return ierr
	}
}
