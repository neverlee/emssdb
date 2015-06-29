package emssdb

import (
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"runtime"
)

const (
	FORWARD  = 0
	BACKWARD = 1
)

//type IteratorIf interface {
//	Skip(offset uint64) bool
//	Next() bool
//	Key() Bytes
//	Value() Bytes
//}

type Iterator struct {
	it        iterator.Iterator
	direction int
	key       Bytes
	value     Bytes
}

func NewIterator(it iterator.Iterator, direction int) (that *Iterator) {
	this := Iterator{it, direction, nil, nil}
	runtime.SetFinalizer(&this,
		func(this *Iterator) {
			this.it.Release()
		})
	return &this
}

func (this *Iterator) Key() (ret Bytes) {
	return this.key
}

func (this *Iterator) Value() (ret Bytes) {
	return this.value
}

func (this *Iterator) Skip(offset uint64) (ret bool) {
	var b bool
	for ; offset > 0; offset-- {
		if b = this.Next(); !b {
			return false
		}
	}
	return true
}

func (this *Iterator) next() (ret bool) {
	it := this.it
	if this.direction == FORWARD {
		return it.Next()
	} else {
		return it.Prev()
	}
}

func (this *Iterator) Next() (ret bool) {
	b := this.next()
	if b {
		this.key = NewByClone(this.it.Key())
		this.value = NewByClone(this.it.Value())
	} else {
		this.key = nil
		this.value = nil
	}
	return b
}

type KIterator struct {
	*Iterator
}

func NewKIterator(it *Iterator) (ret *KIterator) {
	var this KIterator
	this.Iterator = it
	return &this
}

func (this *KIterator) Next() (ret bool) {
	b := this.next()
	if b {
		this.key = NewByClone(this.it.Key()[1:])
		this.value = NewByClone(this.it.Value())
	} else {
		this.key = nil
		this.value = nil
	}
	return b
}

type HIterator struct {
	*Iterator
}

func NewHIterator(it *Iterator) (ret *HIterator) {
	var this HIterator
	this.Iterator = it
	return &this
}

func (this *HIterator) Next() (ret bool) {
	nb := this.next()
	if nb {
		rawkey := this.it.Key()
		_, this.key = decodeHashKey(rawkey)
		this.key = NewByClone(this.key)
		this.value = NewByClone(this.it.Value())
	} else {
		this.key = nil
		this.value = nil
	}
	return nb
}

type QIterator struct {
	*Iterator
	key int64
}

func NewQIterator(it *Iterator) (ret *QIterator) {
	var this QIterator
	this.Iterator = it
	return &this
}

func (this *QIterator) Next() (ret bool) {
	nb := this.next()
	if nb {
		rawkey := this.it.Key()
		_, this.key = decodeQitemKey(rawkey)
		this.value = NewByClone(this.it.Value())
	} else {
		this.key = -1
		this.value = nil
	}
	return nb
}

func (this *QIterator) Key() (ret int64) {
	return this.key
}

///***** ZSET *****/
type ZIterator struct {
	*Iterator
	score int64
}

func NewZIterator(it *Iterator) (ret *ZIterator) {
	var this ZIterator
	this.Iterator = it
	this.score = -1
	return &this
}

func (this *ZIterator) Next() (ret bool) {
	nb := this.next()
	if nb {
		rawkey := this.it.Key()
		key, value, score := decodeZscoreKey(rawkey)
		this.key = NewByClone(key)
		this.value = NewByClone(value)
		this.score = score
	} else {
		this.key = nil
		this.value = nil
		this.score = -1
	}
	return nb
}

func (this *ZIterator) Score() (ret int64) {
	return this.score
}

func (this *ZIterator) Name() (ret Bytes) {
	return this.key
}

func (this *ZIterator) Key() (ret Bytes) {
	return this.value
}

func (this *ZIterator) Value() {
}
