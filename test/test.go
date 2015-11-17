package main

import (
	"bufio"
	"fmt"
	"github.com/neverlee/emssdb"
	"os"
)

func bkey(key string) (rk []byte) {
	k1 := []byte(key)
	if key == "-" || key == "" {
		k1 = nil
	}
	return k1
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s dbpath\n", os.Args[0])
		return
	}
	opt := emssdb.Options{os.Args[1], 4, true}
	db, err := emssdb.OpenDB(opt)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("emSSDB >> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(line, err)
			continue
		}
		var cmd, key, key2, value string
		var ival, ival2 int64

		fmt.Sscanf(line, "%s", &cmd)
		switch cmd {
		//raw
		case "rawset":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &value)
			fmt.Println(db.RawSet(emssdb.Bytes(key), emssdb.Bytes(value)))
		case "rawget":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			g, e := db.RawGet(emssdb.Bytes(key))
			fmt.Println(string(g), e)
		case "rawdel":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			fmt.Println(db.RawDel(emssdb.Bytes(key)))
		case "rawscan":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			k1, k2 := bkey(key), bkey(key2)
			it := db.Iterator(k1, k2)
			for it.Next() {
				fmt.Println(string(it.Key()), string(it.Value()), "\t", it.Key(), "\t", it.Value())
			}
		case "rawclear":
			it := db.Iterator(nil, nil)
			for it.Next() {
				db.RawDel(it.Key())
			}
			fmt.Println("Clear all done!")
		// kv
		case "set":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &value)
			fmt.Println(db.Set(emssdb.Bytes(key), emssdb.Bytes(value)))
		case "get":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			g, e := db.Get(emssdb.Bytes(key))
			fmt.Println(string(g), e)
		case "del":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			fmt.Println(db.Del(emssdb.Bytes(key)))
		case "incr":
			fmt.Sscanf(line, "%s%s%d", &cmd, &key, &ival)
			fmt.Println(db.Incr(emssdb.Bytes(key), ival))
		case "scan", "rscan":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			k1, k2 := bkey(key), bkey(key2)
			var kit *emssdb.KIterator
			if cmd == "rscan" {
				kit = db.Rscan(k1, k2)
			} else {
				kit = db.Scan(k1, k2)
			}
			for kit.Next() {
				fmt.Println(string(kit.Key()), string(kit.Value()))
			}
		// Hash
		case "hset":
			fmt.Sscanf(line, "%s%s%s%s", &cmd, &key, &key2, &value)
			fmt.Println(db.Hset(emssdb.Bytes(key), emssdb.Bytes(key2), emssdb.Bytes(value)))
		case "hget":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			g, e := db.Hget(emssdb.Bytes(key), emssdb.Bytes(key2))
			fmt.Println(string(g), e)
		case "hdel":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			fmt.Println(db.Hdel(emssdb.Bytes(key), emssdb.Bytes(key2)))
		case "hincr":
			fmt.Sscanf(line, "%s%s%s%d", &cmd, &key, &key2, &ival)
			fmt.Println(db.Hincr(emssdb.Bytes(key), emssdb.Bytes(key2), ival))
		case "hsize":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			fmt.Println(db.Hsize(emssdb.Bytes(key)))
		case "hlist":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			k1, k2 := bkey(key), bkey(key2)
			hlist := db.Hlist(k1, k2)
			for idx, item := range hlist {
				fmt.Println(idx, string(item))
			}
		case "hscan", "hrscan":
			fmt.Sscanf(line, "%s%s%s%s", &cmd, &value, &key, &key2)
			k1, k2 := bkey(key), bkey(key2)
			var hit *emssdb.HIterator
			if cmd == "hrscan" {
				hit = db.Hrscan(emssdb.Bytes(value), k1, k2)
			} else {
				hit = db.Hscan(emssdb.Bytes(value), k1, k2)
			}
			for hit.Next() {
				fmt.Println(string(hit.Key()), string(hit.Value()))
			}
		// Queue
		case "qget":
			fmt.Sscanf(line, "%s%s%d", &cmd, &key, &ival)
			g, e := db.Qget(emssdb.Bytes(key), ival)
			fmt.Println(string(g), e)
		case "qsize":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			fmt.Println(db.Qsize(emssdb.Bytes(key)))
		case "qscan":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			qit := db.Qscan(emssdb.Bytes(key))
			for qit.Next() {
				fmt.Println(qit.Key(), string(qit.Value()))
			}
		case "qfront":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			g, e := db.Qfront(emssdb.Bytes(key))
			fmt.Println(string(g), e)
		case "qback":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			g, e := db.Qback(emssdb.Bytes(key))
			fmt.Println(string(g), e)
		case "qpopfront":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			g, e := db.QpopFront(emssdb.Bytes(key))
			fmt.Println(string(g), e)
		case "qpopback":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			g, e := db.QpopBack(emssdb.Bytes(key))
			fmt.Println(string(g), e)
		case "qpushfront":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			fmt.Println(db.QpushFront(emssdb.Bytes(key), emssdb.Bytes(key2)))
		case "qpushback":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			fmt.Println(db.QpushBack(emssdb.Bytes(key), emssdb.Bytes(key2)))
		case "qlist":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			k1, k2 := bkey(key), bkey(key2)
			hlist := db.Qlist(k1, k2)
			for idx, item := range hlist {
				fmt.Println(idx, string(item))
			}
		// ZSET
		case "zget":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			fmt.Println(db.Zget(emssdb.Bytes(key), emssdb.Bytes(key2)))
		case "zset":
			fmt.Sscanf(line, "%s%s%s%d", &cmd, &key, &key2, &ival)
			fmt.Println(db.Zset(emssdb.Bytes(key), emssdb.Bytes(key2), ival))
		case "zdel":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			fmt.Println(db.Zdel(emssdb.Bytes(key), emssdb.Bytes(key2)))
		case "zincr":
			fmt.Sscanf(line, "%s%s%s%d", &cmd, &key, &key2, &ival)
			fmt.Println(db.Zincr(emssdb.Bytes(key), emssdb.Bytes(key2), ival))
		case "zsize":
			fmt.Sscanf(line, "%s%s", &cmd, &key)
			fmt.Println(db.Zsize(emssdb.Bytes(key)))
		case "zlist":
			fmt.Sscanf(line, "%s%s%s", &cmd, &key, &key2)
			k1, k2 := bkey(key), bkey(key2)
			zlist := db.Zlist(k1, k2)
			for idx, item := range zlist {
				fmt.Println(idx, string(item))
			}
		case "zscan", "zrscan":
			fmt.Sscanf(line, "%s%s%d%d", &cmd, &key, &ival, &ival2)
			var zit *emssdb.ZIterator
			if cmd == "zrscan" {
				zit = db.Zrscan(emssdb.Bytes(key), ival, ival2)
			} else {
				zit = db.Zscan(emssdb.Bytes(key), ival, ival2)
			}
			for zit.Next() {
				fmt.Println(string(zit.Name()), string(zit.Key()), zit.Score())
			}
		}
	}
}
