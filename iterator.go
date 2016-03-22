package emssdb

import (
	"github.com/syndtr/goleveldb/leveldb/iterator"
	//"runtime"
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
	//runtime.SetFinalizer(&this,
	//	func(this *Iterator) {
	//		this.it.Release()
	//	})
	return &this
}

func (it *Iterator) Close() {
	it.it.Release()
}

func (it *Iterator) Key() (ret Bytes) {
	return it.key
}

func (it *Iterator) Value() (ret Bytes) {
	return it.value
}

func (it *Iterator) Skip(offset uint64) (ret bool) {
	var b bool
	for ; offset > 0; offset-- {
		if b = it.Next(); !b {
			return false
		}
	}
	return true
}

func (it *Iterator) next() (ret bool) {
	rit := it.it
	if it.direction == FORWARD {
		return rit.Next()
	} else {
		return rit.Prev()
	}
}

func (it *Iterator) Next() (ret bool) {
	b := it.next()
	if b {
		it.key = NewByClone(it.it.Key())
		it.value = NewByClone(it.it.Value())
	} else {
		it.key = nil
		it.value = nil
	}
	return b
}

type KIterator struct {
	*Iterator
}

func NewKIterator(it *Iterator) (ret *KIterator) {
	var kit KIterator
	kit.Iterator = it
	return &kit
}

func (kit *KIterator) Next() (ret bool) {
	b := kit.next()
	if b {
		kit.key = NewByClone(kit.it.Key()[1:])
		kit.value = NewByClone(kit.it.Value())
	} else {
		kit.key = nil
		kit.value = nil
	}
	return b
}

type EIterator struct {
	*Iterator
	etime uint64
}

func NewEIterator(it *Iterator) (ret *EIterator) {
	var eit EIterator
	eit.Iterator = it
	return &eit
}

func (eit *EIterator) Etime() (ret uint64) {
	return eit.etime
}

func (eit *EIterator) Next() (ret bool) {
	b := eit.next()
	if b {
		eit.key = NewByClone(eit.it.Key()[1:])
		v, e := decodeExkvValue(eit.it.Value())
		eit.value = NewByClone(v)
		eit.etime = e
	} else {
		eit.key = nil
		eit.value = nil
		eit.etime = 0
	}
	return b
}

type XIterator struct {
	*Iterator
	etime uint64
}

func NewXIterator(it *Iterator) (ret *XIterator) {
	var xit XIterator
	xit.Iterator = it
	return &xit
}

func (xit *XIterator) Etime() (ret uint64) {
	return xit.etime
}

func (xit *XIterator) Next() (ret bool) {
	b := xit.next()
	if b {
		k, e := decodeExstampKey(xit.it.Key())
		xit.key = NewByClone(k)
		xit.value = nil
		xit.etime = e
	} else {
		xit.key = nil
		xit.value = nil
		xit.etime = 0
	}
	return b
}

type HIterator struct {
	*Iterator
}

func NewHIterator(it *Iterator) (ret *HIterator) {
	var hit HIterator
	hit.Iterator = it
	return &hit
}

func (hit *HIterator) Next() (ret bool) {
	nb := hit.next()
	if nb {
		rawkey := hit.it.Key()
		_, hit.key = decodeHashKey(rawkey)
		hit.key = NewByClone(hit.key)
		hit.value = NewByClone(hit.it.Value())
	} else {
		hit.key = nil
		hit.value = nil
	}
	return nb
}

type QIterator struct {
	*Iterator
	key int64
}

func NewQIterator(it *Iterator) (ret *QIterator) {
	var qit QIterator
	qit.Iterator = it
	return &qit
}

func (qit *QIterator) Next() (ret bool) {
	nb := qit.next()
	if nb {
		rawkey := qit.it.Key()
		_, qit.key = decodeQitemKey(rawkey)
		qit.value = NewByClone(qit.it.Value())
	} else {
		qit.key = -1
		qit.value = nil
	}
	return nb
}

func (qit *QIterator) Key() (ret int64) {
	return qit.key
}

///***** ZSET *****/
type ZIterator struct {
	*Iterator
	score int64
}

func NewZIterator(it *Iterator) (ret *ZIterator) {
	var zit ZIterator
	zit.Iterator = it
	zit.score = -1
	return &zit
}

func (zit *ZIterator) Next() (ret bool) {
	nb := zit.next()
	if nb {
		rawkey := zit.it.Key()
		key, value, score := decodeZscoreKey(rawkey)
		zit.key = NewByClone(key)
		zit.value = NewByClone(value)
		zit.score = score
	} else {
		zit.key = nil
		zit.value = nil
		zit.score = -1
	}
	return nb
}

func (zit *ZIterator) Score() (ret int64) {
	return zit.score
}

func (zit *ZIterator) Name() (ret Bytes) {
	return zit.key
}

func (zit *ZIterator) Key() (ret Bytes) {
	return zit.value
}

func (zit *ZIterator) Value() {
}
