package quickbolt

import (
	"sync"
	"time"
)

const (
	rootBucket           = "root"
	defaultBufferTimeout = time.Second * 1
)

var (
	logMutex sync.Mutex // logMutex functions as a rate limiter for writes to the logger.
)
