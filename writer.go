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
	var w Writer
	w.db = db
	return &w
}

func (w *Writer) Begin() {
	w.batch.Reset()
}

func (w *Writer) RollBack() {
	w.batch.Reset()
}

func (w *Writer) Commit() (err error) {
	//var writeOpts opt.WriteOptions
	return w.db.Write(&w.batch, nil)
}

func (w *Writer) Put(key []byte, val []byte) {
	w.batch.Put(key, val)
}

func (w *Writer) Delete(key []byte) {
	w.batch.Delete(key)
}

func (w *Writer) Do() {
	w.Mutex.Lock()
	w.Begin()
}

func (w *Writer) Done() {
	w.RollBack()
	w.Mutex.Unlock()
}
