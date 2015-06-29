package emssdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"runtime"
)

type DB struct {
	db      *leveldb.DB
	options opt.Options
	writer  *Writer
}

func NewDB() *DB {
	var db DB
	return &db
}

func OpenDB(options Options) (that *DB, err error) {
	main_db_path := options.Path
	cache_size := options.CacheSize
	write_buffer_size := 4
	block_size := 4
	compression := options.Compression

	if cache_size <= 0 {
		cache_size = 8
	}

	//log::path,cache_size,block_size,write_buffer,compression

	this := NewDB()
	this.options.ErrorIfMissing = false
	this.options.Filter = filter.NewBloomFilter(10)
	//this.Options.BlockCacher = leveldb::NewLRUCache(cache_size * 1048576)
	this.options.BlockSize = block_size * 1024
	this.options.WriteBuffer = write_buffer_size * 1024 * 1024
	if compression {
		this.options.Compression = opt.SnappyCompression
	} else {
		this.options.Compression = opt.NoCompression
	}

	if tdb, err := leveldb.OpenFile(main_db_path, &this.options); err == nil {
		runtime.SetFinalizer(this,
			func(this *DB) {
				this.db.Close()
			})
		this.db = tdb
		this.writer = NewWriter(this.db)
		return this, nil
	} else {
		return nil, err
	}
}

//	// return (start, end], not include start
func (this *DB) Iterator(start Bytes, end Bytes) (ret *Iterator) {
	if len(start) == 0 {
		start = nil
	}
	if len(end) == 0 {
		end = nil
	}
	var iopt opt.ReadOptions
	iopt.DontFillCache = true
	it := this.db.NewIterator(&util.Range{Start: start, Limit: end}, &iopt)
	return NewIterator(it, FORWARD)
}

func (this *DB) RevIterator(start Bytes, end Bytes) (ret *Iterator) {
	if len(start) == 0 {
		start = nil
	}
	if len(end) == 0 {
		end = nil
	}
	var iopt opt.ReadOptions
	iopt.DontFillCache = true
	it := this.db.NewIterator(&util.Range{Start: start, Limit: end}, &iopt)
	it.Last()
	it.Next()
	return NewIterator(it, BACKWARD)
}

func (this *DB) Info() (ret map[string]string) {
	//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
	//     where <N> is an ASCII representation of a level number (e.g. "0").
	//  "leveldb.stats" - returns a multi-line string that describes statistics
	//     about the internal operation of the DB.
	//  "leveldb.sstables" - returns a multi-line string that describes all
	//     of the sstables that make up the db contents.
	var keys []string
	info := make(map[string]string)

	//for i := 0; i < 7; i++ {
	//	s := fmt.Sprintf("leveldb.num-files-at-level%d", i)
	//	keys = append(keys, s)
	//}

	keys = append(keys, "leveldb.stats")
	//keys = append(keys, "leveldb.sstables")

	for _, key := range keys {
		if val, err := this.db.GetProperty(key); err == nil {
			info[key] = val
		}
	}

	return info
}

func (this *DB) Compact() (err error) {
	return this.db.CompactRange(util.Range{})
}

func (this *DB) KeyRange(keys []string) {
	//	int ret = 0;
	//	std::string kstart, kend;
	//	std::string hstart, hend;
	//	std::string zstart, zend;
	//
	//	Iterator *it;
	//
	//	it = this->iterator(encode_kv_key(""), "", 1);
	//	if(it->next()){
	//		Bytes ks = it->key();
	//		if(ks.data()[0] == DataType::KV){
	//			std::string n;
	//			if(decode_kv_key(ks, &n) == -1){
	//				ret = -1;
	//			}else{
	//				kstart = n;
	//			}
	//		}
	//	}
	//	delete it;
	//
	//	it = this->rev_iterator(encode_kv_key("\xff"), "", 1);
	//	if(it->next()){
	//		Bytes ks = it->key();
	//		if(ks.data()[0] == DataType::KV){
	//			std::string n;
	//			if(decode_kv_key(ks, &n) == -1){
	//				ret = -1;
	//			}else{
	//				kend = n;
	//			}
	//		}
	//	}
	//	delete it;
	//
	//	it = this->iterator(encode_hsize_key(""), "", 1);
	//	if(it->next()){
	//		Bytes ks = it->key();
	//		if(ks.data()[0] == DataType::HSIZE){
	//			std::string n;
	//			if(decode_hsize_key(ks, &n) == -1){
	//				ret = -1;
	//			}else{
	//				hstart = n;
	//			}
	//		}
	//	}
	//	delete it;
	//
	//	it = this->rev_iterator(encode_hsize_key("\xff"), "", 1);
	//	if(it->next()){
	//		Bytes ks = it->key();
	//		if(ks.data()[0] == DataType::HSIZE){
	//			std::string n;
	//			if(decode_hsize_key(ks, &n) == -1){
	//				ret = -1;
	//			}else{
	//				hend = n;
	//			}
	//		}
	//	}
	//	delete it;
	//
	//	it = this->iterator(encode_zsize_key(""), "", 1);
	//	if(it->next()){
	//		Bytes ks = it->key();
	//		if(ks.data()[0] == DataType::ZSIZE){
	//			std::string n;
	//			if(decode_hsize_key(ks, &n) == -1){
	//				ret = -1;
	//			}else{
	//				zstart = n;
	//			}
	//		}
	//	}
	//	delete it;
	//
	//	it = this->rev_iterator(encode_zsize_key("\xff"), "", 1);
	//	if(it->next()){
	//		Bytes ks = it->key();
	//		if(ks.data()[0] == DataType::ZSIZE){
	//			std::string n;
	//			if(decode_hsize_key(ks, &n) == -1){
	//				ret = -1;
	//			}else{
	//				zend = n;
	//			}
	//		}
	//	}
	//	delete it;
	//
	//	keys->push_back(kstart);
	//	keys->push_back(kend);
	//	keys->push_back(hstart);
	//	keys->push_back(hend);
	//	keys->push_back(zstart);
	//	keys->push_back(zend);
	//
	//	return ret;
}

//
//	/* raw operates */
//
//	// repl: whether to sync this operation to slaves
func (this *DB) RawSet(key Bytes, val Bytes) (err error) {
	//var writeOpts opt.WriteOptions
	return this.db.Put(key, val, nil)
}
func (this *DB) RawDel(key Bytes) (err error) {
	//var writeOpts opt.WriteOptions
	return this.db.Delete(key, nil)
}
func (this *DB) RawGet(key Bytes) (val Bytes, err error) {
	//var writeOpts opt.WriteOptions
	return this.db.Get(key, nil)
}
