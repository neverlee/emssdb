package emssdb

import (
	"encoding/binary"
)

type Bytes []byte
type B = Bytes

// NewByClone deep copy a new byte slice
func NewByClone(bb []byte) (ret Bytes) {
	a := make(Bytes, len(bb))
	copy(a, bb)
	return a
}

// NewByUInt64 by bigendian
func NewByUInt64(i uint64) (ret Bytes) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

// NewByInt64 same as NewByUInt64
func NewByInt64(i int64) (ret Bytes) {
	return NewByUInt64(uint64(i))
}

// GetUInt64 to uint64 by the first 8 bytes(bigendian)
func (b Bytes) GetUInt64() (ret uint64) {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b[:8])
}

// GetInt64 same as GetUInt64
func (b Bytes) GetInt64() (ret int64) {
	return int64(b.GetUInt64())
}
