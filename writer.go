package emssdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

type Writer struct {
	db    *leveldb.DB
	batch leveldb.Batch
	Mutex sync.Mutex
}

func NewWriter(db *leveldb.DB) *Writer {
	var this Writer
	this.db = db
	return &this
}

func (this *Writer) Begin() {
	this.batch.Reset()
}

func (this *Writer) RollBack() {
	this.batch.Reset()
}

func (this *Writer) Commit() (err error) {
	//var writeOpts opt.WriteOptions
	return this.db.Write(&this.batch, nil)
}

func (this *Writer) Put(key []byte, val []byte) {
	this.batch.Put(key, val)
}

func (this *Writer) Delete(key []byte) {
	this.batch.Delete(key)
}

func (this *Writer) Do() {
	this.Mutex.Lock()
	this.Begin()
}

func (this *Writer) Done() {
	this.RollBack()
	this.Mutex.Unlock()
}
