package emssdb

import (
	"time"
)

type Options struct {
	Path        string
	CacheSize   int
	Compression bool
	ExpireDelay time.Duration
}
