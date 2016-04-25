package emssdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	//"runtime"
	"sync"
	"time"
)

type DB struct {
	db          *leveldb.DB
	options     opt.Options
	writer      *Writer
	expireDelay time.Duration
	end         bool
	waitgroup   sync.WaitGroup
}

func newDB() *DB {
	var db DB
	return &db
}

func OpenDB(options Options) (that *DB, err error) {
	mainDBPath := options.Path
	cacheSize := options.CacheSize
	writeBufferSize := 4
	blockSize := 4
	compression := options.Compression
	if options.ExpireDelay <= time.Second {
		options.ExpireDelay = time.Second
	}

	if cacheSize <= 0 {
		cacheSize = 8
	}

	//log::path,cacheSize,blockSize,write_buffer,compression

	d := newDB()
	d.options.ErrorIfMissing = false
	d.options.Filter = filter.NewBloomFilter(10)
	//d.Options.BlockCacher = leveldb::NewLRUCache(cacheSize * 1048576)
	d.options.BlockCacheCapacity = cacheSize * 1024 * 1024
	d.options.BlockSize = blockSize * 1024
	d.options.WriteBuffer = writeBufferSize * 1024 * 1024
	d.expireDelay = options.ExpireDelay
	if compression {
		d.options.Compression = opt.SnappyCompression
	} else {
		d.options.Compression = opt.NoCompression
	}

	if tdb, err := leveldb.OpenFile(mainDBPath, &d.options); err == nil {
		//runtime.SetFinalizer(d,
		//	func(d *DB) {
		//		d.db.Close()
		//	})
		d.db = tdb
		d.writer = NewWriter(d.db)
		go d.expireDaemon()
		return d, nil
	} else {
		return nil, err
	}
}

func (d *DB) Close() {
	d.end = true
	d.waitgroup.Wait()
	d.db.Close()
}

//	// return (start, end], not include start
func (d *DB) Iterator(start Bytes, end Bytes) (ret *Iterator) {
	if len(start) == 0 {
		start = nil
	}
	if len(end) == 0 {
		end = nil
	}
	var iopt opt.ReadOptions
	iopt.DontFillCache = true
	it := d.db.NewIterator(&util.Range{Start: start, Limit: end}, &iopt)
	return NewIterator(it, FORWARD)
}

func (d *DB) RevIterator(start Bytes, end Bytes) (ret *Iterator) {
	if len(start) == 0 {
		start = nil
	}
	if len(end) == 0 {
		end = nil
	}
	var iopt opt.ReadOptions
	iopt.DontFillCache = true
	it := d.db.NewIterator(&util.Range{Start: start, Limit: end}, &iopt)
	it.Last()
	it.Next()
	return NewIterator(it, BACKWARD)
}

func (d *DB) Info() (ret map[string]string) {
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
		if val, err := d.db.GetProperty(key); err == nil {
			info[key] = val
		}
	}

	return info
}

func (d *DB) Compact() (err error) {
	return d.db.CompactRange(util.Range{})
}

func (d *DB) KeyRange(keys []string) {
	//	int ret = 0;
	//	std::string kstart, kend;
	//	std::string hstart, hend;
	//	std::string zstart, zend;
	//
	//	Iterator *it;
	//
	//	it = d->iterator(encode_kv_key(""), "", 1);
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
	//	it = d->rev_iterator(encode_kv_key("\xff"), "", 1);
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
	//	it = d->iterator(encode_hsize_key(""), "", 1);
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
	//	it = d->rev_iterator(encode_hsize_key("\xff"), "", 1);
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
	//	it = d->iterator(encode_zsize_key(""), "", 1);
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
	//	it = d->rev_iterator(encode_zsize_key("\xff"), "", 1);
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
//	// repl: whether to sync d operation to slaves
func (d *DB) RawSet(key Bytes, val Bytes) (err error) {
	//var writeOpts opt.WriteOptions
	return d.db.Put(key, val, nil)
}
func (d *DB) RawDel(key Bytes) (err error) {
	//var writeOpts opt.WriteOptions
	return d.db.Delete(key, nil)
}
func (d *DB) RawGet(key Bytes) (val Bytes, err error) {
	//var writeOpts opt.WriteOptions
	return d.db.Get(key, nil)
}
