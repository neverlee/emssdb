package emssdb

import (
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	qFRONT_SEQ     = -1
	qBACK_SEQ      = -2
	qBITMOD        = 0x0FFFFFFFffffffff
	qITEM_SEQ_INIT = qBITMOD / 2
)

func encodeQsizeKey(name Bytes) (ret Bytes) {
	return encodeOneKey(DTQSIZE, name)
}

func decodeQsizeKey(slice Bytes) (ret Bytes) {
	return decodeOneKey(slice)
}

func encodeQitemKey(name Bytes, seq int64) (ret Bytes) {
	buf := make(Bytes, 1+len(name)+1+8)
	buf[0] = DTQUEUE
	copy(buf[1:], name)
	buf[1+len(name)] = 0
	binary.BigEndian.PutUint64(buf[2+len(name):], uint64(seq))
	return buf
}

func decodeQitemKey(slice Bytes) (name Bytes, seq int64) {
	if len(slice) < 10 {
		return nil, 0
	}
	gname := slice[1 : len(slice)-9]
	gseq := Bytes(slice[len(slice)-8:]).GetInt64()
	return gname, gseq
}

func (db *DB) Qget(name Bytes, seq int64) (ret Bytes, err error) {
	// readoption
	rkey := encodeQitemKey(name, seq)
	return db.db.Get(rkey, nil)
}

func (db *DB) qgetint64(name Bytes, seq int64) (ret int64, err error) {
	// readoption
	rkey := encodeQitemKey(name, seq)
	if val, err := db.db.Get(rkey, nil); err == nil {
		if len(val) != 8 {
			return 0, ErrNotIntVal
		} else {
			ival := Bytes(val).GetInt64()
			return ival, nil
		}
	} else {
		return 0, err
	}
}

func (db *DB) qdelOne(name Bytes, seq int64) (err error) {
	rkey := encodeQitemKey(name, seq)
	db.writer.Delete(rkey)
	return nil
}

func (db *DB) qsetOne(name Bytes, seq int64, item Bytes) (err error) {
	rkey := encodeQitemKey(name, seq)
	db.writer.Put(rkey, item)
	return nil
}

func (db *DB) qsetInt(name Bytes, seq int64, item int64) (err error) {
	rkey := encodeQitemKey(name, seq)
	db.writer.Put(rkey, NewByInt64(item))
	return nil
}

func (db *DB) qsetSize(name Bytes, isize int64) (err error) {
	writer := db.writer
	skey := encodeQsizeKey(name)
	if isize == 0 {
		writer.Delete(skey)
	} else {
		buf := NewByInt64(isize)
		writer.Put(skey, buf)
	}
	return nil
}

func (db *DB) Qsize(name Bytes) (ret int64, err error) {
	skey := encodeQsizeKey(name)
	// readoption
	isize := int64(0)
	ssize, err := db.db.Get(skey, nil)
	if err == nil {
		isize = Bytes(ssize).GetInt64()
	}
	return isize, err
}

func (db *DB) Qfront(name Bytes) (ret Bytes, err error) {
	if seq, serr := db.qgetint64(name, qFRONT_SEQ); serr == nil {
		return db.Qget(name, seq)
	} else {
		return nil, serr
	}
}

func (db *DB) Qback(name Bytes) (ret Bytes, err error) {
	if seq, serr := db.qgetint64(name, qBACK_SEQ); serr == nil {
		return db.Qget(name, seq)
	} else {
		return nil, serr
	}
}

func (db *DB) _qpush(name, item Bytes, fbseq int64) (ret error) {
	writer := db.writer
	writer.Do()
	defer writer.Done()

	isize, ierr := db.Qsize(name)
	if ierr != nil && ierr != leveldb.ErrNotFound {
		return ierr
	}
	if isize >= qBITMOD { //  isize+1 >= qMAX_SIZE {
		return ErrOutOfRange
	}
	seq, serr := db.qgetint64(name, fbseq)
	// update front and/or back
	if serr == leveldb.ErrNotFound {
		seq = qITEM_SEQ_INIT
		db.qsetInt(name, qFRONT_SEQ, seq)
		db.qsetInt(name, qBACK_SEQ, seq)
	} else if serr == nil {
		if fbseq == qFRONT_SEQ {
			seq = (seq + 1) & qBITMOD
		} else {
			seq = (seq - 1) & qBITMOD
		}
		db.qsetInt(name, fbseq, seq)
	} else {
		return serr
	}

	// insert item
	db.qsetOne(name, seq, item)
	// change queue size
	db.qsetSize(name, isize+1)
	return writer.Commit()
}

func (db *DB) QpushFront(name, item Bytes) (ret error) {
	return db._qpush(name, item, qFRONT_SEQ)
}

func (db *DB) QpushBack(name, item Bytes) (ret error) {
	return db._qpush(name, item, qBACK_SEQ)
}

func (db *DB) _qpop(name Bytes, fbseq int64) (item Bytes, ret error) {
	writer := db.writer
	writer.Do()
	defer writer.Done()

	isize, ierr := db.Qsize(name)
	if ierr != nil && ierr != leveldb.ErrNotFound {
		return nil, ierr
	}
	//if isize < 1 { return ErrOutOfRange }
	seq, serr := db.qgetint64(name, fbseq)
	if serr != nil {
		return nil, serr
	}

	gitem, gerr := db.Qget(name, seq)
	if gerr != nil {
		return gitem, gerr
	}

	db.qdelOne(name, seq)
	isize--
	if isize <= 0 {
		db.qdelOne(name, qFRONT_SEQ)
		db.qdelOne(name, qBACK_SEQ)
	} else {
		if fbseq == qFRONT_SEQ {
			seq = (seq - 1) & qBITMOD
		} else {
			seq = (seq + 1) & qBITMOD
		}
		db.qsetInt(name, fbseq, seq)
	}
	db.qsetSize(name, isize)

	return gitem, writer.Commit()
}

func (db *DB) QpopFront(name Bytes) (item Bytes, ret error) {
	return db._qpop(name, qFRONT_SEQ)
}

func (db *DB) QpopBack(name Bytes) (item Bytes, ret error) {
	return db._qpop(name, qBACK_SEQ)
}

func (db *DB) Qlist(sname, ename Bytes) (ret []Bytes) {
	start, end := encodeQsizeKey(sname), encodeQsizeKey(ename)
	if len(ename) == 0 {
		end = encodeOneKey(DTQSIZE+1, nil)
	}
	it := db.Iterator(start, end)
	var list = make([]Bytes, 0)
	for it.Next() {
		ks := it.Key()
		ks = ks[1:]
		list = append(list, ks)
	}
	return list
}

func encodeQitemiteraKey(name Bytes, fill byte) (ret Bytes) {
	buf := make(Bytes, 1+len(name)+1)
	buf[0] = DTQUEUE
	copy(buf[1:], name)
	buf[1+len(name)] = fill
	return buf
}

func (db *DB) Qscan(name Bytes) (ret *QIterator) {
	//key_start, key_end := encodeQitemiteraKey(name, 0), encodeQitemiteraKey(name, 1)
	keyStart, keyEnd := encodeQitemKey(name, 0), encodeQitemKey(name, 0x7FFFFFFFffffffff)
	return NewQIterator(db.Iterator(keyStart, keyEnd))
}
