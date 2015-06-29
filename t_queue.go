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

func (this *DB) Qget(name Bytes, seq int64) (ret Bytes, err error) {
	// readoption
	rkey := encodeQitemKey(name, seq)
	return this.db.Get(rkey, nil)
}

func (this *DB) qgetint64(name Bytes, seq int64) (ret int64, err error) {
	// readoption
	rkey := encodeQitemKey(name, seq)
	if val, err := this.db.Get(rkey, nil); err == nil {
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

func (this *DB) qdelOne(name Bytes, seq int64) (err error) {
	rkey := encodeQitemKey(name, seq)
	this.writer.Delete(rkey)
	return nil
}

func (this *DB) qsetOne(name Bytes, seq int64, item Bytes) (err error) {
	rkey := encodeQitemKey(name, seq)
	this.writer.Put(rkey, item)
	return nil
}

func (this *DB) qsetInt(name Bytes, seq int64, item int64) (err error) {
	rkey := encodeQitemKey(name, seq)
	this.writer.Put(rkey, NewByInt64(item))
	return nil
}

func (this *DB) qsetSize(name Bytes, isize int64) (err error) {
	writer := this.writer
	skey := encodeQsizeKey(name)
	if isize == 0 {
		writer.Delete(skey)
	} else {
		buf := NewByInt64(isize)
		writer.Put(skey, buf)
	}
	return nil
}

func (this *DB) Qsize(name Bytes) (ret int64, err error) {
	skey := encodeQsizeKey(name)
	// readoption
	isize := int64(0)
	ssize, err := this.db.Get(skey, nil)
	if err == nil {
		isize = Bytes(ssize).GetInt64()
	}
	return isize, err
}

func (this *DB) Qfront(name Bytes) (ret Bytes, err error) {
	if seq, serr := this.qgetint64(name, qFRONT_SEQ); serr == nil {
		return this.Qget(name, seq)
	} else {
		return nil, serr
	}
}

func (this *DB) Qback(name Bytes) (ret Bytes, err error) {
	if seq, serr := this.qgetint64(name, qBACK_SEQ); serr == nil {
		return this.Qget(name, seq)
	} else {
		return nil, serr
	}
}

func (this *DB) _qpush(name, item Bytes, fbseq int64) (ret error) {
	writer := this.writer
	writer.Do()
	defer writer.Done()

	isize, ierr := this.Qsize(name)
	if ierr != nil && ierr != leveldb.ErrNotFound {
		return ierr
	}
	if isize >= qBITMOD { //  isize+1 >= qMAX_SIZE {
		return ErrOutOfRange
	}
	seq, serr := this.qgetint64(name, fbseq)
	// update front and/or back
	if serr == leveldb.ErrNotFound {
		seq = qITEM_SEQ_INIT
		this.qsetInt(name, qFRONT_SEQ, seq)
		this.qsetInt(name, qBACK_SEQ, seq)
	} else if serr == nil {
		if fbseq == qFRONT_SEQ {
			seq = (seq + 1) & qBITMOD
		} else {
			seq = (seq - 1) & qBITMOD
		}
		this.qsetInt(name, fbseq, seq)
	} else {
		return serr
	}

	// insert item
	this.qsetOne(name, seq, item)
	// change queue size
	this.qsetSize(name, isize+1)
	return writer.Commit()
}

func (this *DB) QpushFront(name, item Bytes) (ret error) {
	return this._qpush(name, item, qFRONT_SEQ)
}

func (this *DB) QpushBack(name, item Bytes) (ret error) {
	return this._qpush(name, item, qBACK_SEQ)
}

func (this *DB) _qpop(name Bytes, fbseq int64) (item Bytes, ret error) {
	writer := this.writer
	writer.Do()
	defer writer.Done()

	isize, ierr := this.Qsize(name)
	if ierr != nil && ierr != leveldb.ErrNotFound {
		return nil, ierr
	}
	//if isize < 1 { return ErrOutOfRange }
	seq, serr := this.qgetint64(name, fbseq)
	if serr != nil {
		return nil, serr
	}

	gitem, gerr := this.Qget(name, seq)
	if gerr != nil {
		return gitem, gerr
	}

	this.qdelOne(name, seq)
	isize -= 1
	if isize <= 0 {
		this.qdelOne(name, qFRONT_SEQ)
		this.qdelOne(name, qBACK_SEQ)
	} else {
		if fbseq == qFRONT_SEQ {
			seq = (seq - 1) & qBITMOD
		} else {
			seq = (seq + 1) & qBITMOD
		}
		this.qsetInt(name, fbseq, seq)
	}
	this.qsetSize(name, isize)

	return gitem, writer.Commit()
}

func (this *DB) QpopFront(name Bytes) (item Bytes, ret error) {
	return this._qpop(name, qFRONT_SEQ)
}

func (this *DB) QpopBack(name Bytes) (item Bytes, ret error) {
	return this._qpop(name, qBACK_SEQ)
}

func (this *DB) Qlist(sname, ename Bytes) (ret []Bytes) {
	start, end := encodeQsizeKey(sname), encodeQsizeKey(ename)
	if len(ename) == 0 {
		end = encodeOneKey(DTQSIZE+1, nil)
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

func encodeQitemiteraKey(name Bytes, fill byte) (ret Bytes) {
	buf := make(Bytes, 1+len(name)+1)
	buf[0] = DTQUEUE
	copy(buf[1:], name)
	buf[1+len(name)] = fill
	return buf
}

func (this *DB) Qscan(name Bytes) (ret *QIterator) {
	//key_start, key_end := encodeQitemiteraKey(name, 0), encodeQitemiteraKey(name, 1)
	key_start, key_end := encodeQitemKey(name, 0), encodeQitemKey(name, 0x7FFFFFFFffffffff)
	return NewQIterator(this.Iterator(key_start, key_end))
}
