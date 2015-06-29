package emssdb

import (
	"errors"
)

const (
	UINT64_MAX       = 18446744073709551615
	SSDB_SCORE_WIDTH = 9
	SSDB_KEY_LEN_MAX = 255
	SYNCLOG          = 1
	DTKV             = 'k'
	DTHASH           = 'h' // hashmap(sorted by key)
	DTHSIZE          = 'H'
	DTZSET           = 's' // key => score
	DTZSCORE         = 'z' // key|score => ""
	DTZSIZE          = 'Z'
	DTQUEUE          = 'q'
	DTQSIZE          = 'Q'
	MIN_PREFIX       = DTHASH
	MAX_PREFIX       = DTZSET
)

type Status error

var (
	ErrNotFound   = errors.New("ssdb: not found")
	ErrEmptyKey   = errors.New("ssdb: empty key")
	ErrLongKey    = errors.New("ssdb: key too long")
	StatSuccess   = errors.New("")
	StatSucChange = errors.New("ssdb: change size")
	StatNotFound  = errors.New("ssdb: no item")
	ErrOptFail    = errors.New("ssdb: operate fail")
	ErrNotIntVal  = errors.New("ssdb: not intager val")
	ErrOutOfRange = errors.New("ssdb: out of range")
	ErrQueue      = errors.New("error queue")
	//ErrSnapshotReleased = errors.New("ssdb: snapshot released")
	//ErrClosed           = errors.New("ssdb: closed")
)

/*
static inline double millitime(){
	struct timeval now;
	gettimeofday(&now, NULL);
	double ret = now.tv_sec + now.tv_usec/1000.0/1000.0;
	return ret;
}

static inline int64_t time_ms(){
	struct timeval now;
	gettimeofday(&now, NULL);
	return now.tv_sec * 1000 + now.tv_usec/1000;
}
*/

// [DT][KEY]
func encodeOneKey(dt byte, key Bytes) (ret Bytes) {
	if key == nil {
		key = Bytes{}
	}
	buf := make(Bytes, len(key)+1)
	buf[0] = dt
	copy(buf[1:], key)
	return buf
}

func decodeOneKey(slice Bytes) (key Bytes) {
	if len(slice) > 1 {
		return slice[1:]
	} else {
		return nil
	}
}

// [DT][len(NAME)][NAME][0][KEY]
func encodeTwoKey(dt byte, name Bytes, seq byte, key Bytes) (ret Bytes) {
	length := 1 + 2 + len(name) + len(key)
	buf := make(Bytes, length)
	buf[0] = dt
	//if len(name) > 255 { return nil }
	buf[1] = byte(len(name))
	p := buf[2:]
	copy(buf[2:], name)
	p = p[len(name):]
	p[0] = seq
	p = p[1:]
	copy(p, key)
	return buf
}

func decodeTwoKey(slice Bytes) (keya Bytes, keyb Bytes) {
	if len(slice) < 3 {
		return nil, nil
	}
	buf := slice[2:]
	if uint(slice[1]) > uint(len(buf)) {
		return nil, nil
	}
	keya = buf[:slice[1]]
	keyb = buf[len(keya)+1:]
	return
}

func isVaildHashKey(name, key Bytes) (err error) {
	if len(key) == 0 || len(name) == 0 {
		return ErrEmptyKey
	}
	if len(name) > SSDB_KEY_LEN_MAX || len(key) > SSDB_KEY_LEN_MAX {
		return ErrLongKey
	}
	return nil
}


