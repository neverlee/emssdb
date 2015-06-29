package emssdb

import (
	"encoding/binary"
)

type Bytes []byte

func NewByClone(bb []byte) (ret Bytes) {
	a := make(Bytes, len(bb))
	copy(a, bb)
	return a
}

func NewByUInt64(i uint64) (ret Bytes) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func NewByInt64(i int64) (ret Bytes) {
	return NewByUInt64(uint64(i))
}

func (b Bytes) GetUInt64() (ret uint64) {
	if len(b) < 8 {
		return 0
	} else {
		return binary.BigEndian.Uint64(b[:8])
	}
}

func (b Bytes) GetInt64() (ret int64) {
	return int64(b.GetUInt64())
}
