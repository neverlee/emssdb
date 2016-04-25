package emssdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

// Writer for batch operations
type Writer struct {
	db    *leveldb.DB
	batch leveldb.Batch
	Mutex sync.Mutex
}

// NewWriter return a leveldb batch writer
func NewWriter(db *leveldb.DB) *Writer {
	var w Writer
	w.db = db
	return &w
}

// Begin before batch operation
func (w *Writer) Begin() {
	w.batch.Reset()
}

// RollBack rollback the batch operations
func (w *Writer) RollBack() {
	w.batch.Reset()
}

// Commit commit all operations
func (w *Writer) Commit() (err error) {
	//var writeOpts opt.WriteOptions
	return w.db.Write(&w.batch, nil)
}

// Put add a set operation
func (w *Writer) Put(key []byte, val []byte) {
	w.batch.Put(key, val)
}

// Delete add a delete operation
func (w *Writer) Delete(key []byte) {
	w.batch.Delete(key)
}

// Do try to do all operations
func (w *Writer) Do() {
	w.Mutex.Lock()
	w.Begin()
}

// Done end the batch operations
func (w *Writer) Done() {
	w.RollBack()
	w.Mutex.Unlock()
}
