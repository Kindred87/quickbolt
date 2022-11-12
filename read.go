package quickbolt

import (
	"bytes"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

// getValue returns the value paired with the given key.
// The returned value will be nil if the key could not be found.
//
// If mustExist is true, an error will be returned if the key could not be found.
func getValue(db *bbolt.DB, key []byte, path [][]byte, mustExist bool) ([]byte, error) {
	if db == nil {
		c := withCallerInfo(fmt.Sprintf("value retrieval for %s", key), 3)
		return nil, fmt.Errorf("%s received nil db", c)
	}

	var value []byte

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		} else if bkt == nil {
			return nil
		}

		value = bkt.Get(key)
		if value == nil && mustExist {
			return newErrLocate(fmt.Sprintf("key %s at %#v", string(key), path))
		}

		return nil
	})

	if err != nil {
		c := withCallerInfo(fmt.Sprintf("value retrieval for %s", key), 3)
		return nil, fmt.Errorf("%s experienced error while reading value: %w", c, err)
	}
	return value, nil
}

func getKey(db *bbolt.DB, value []byte, path [][]byte, mustExist bool) ([]byte, error) {
	if db == nil {
		c := withCallerInfo(fmt.Sprintf("key retrieval for %s", value), 3)
		return nil, fmt.Errorf("%s received nil db", c)
	}

	var key []byte

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		} else if bkt == nil {
			return nil
		}

		c := bkt.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if bytes.Equal(v, value) {
				key = k
				return nil
			}
		}

		if key == nil && mustExist {
			return newErrLocate(fmt.Sprintf("value %s at %#v", string(value), path))
		}

		return nil
	})

	if err != nil {
		c := withCallerInfo(fmt.Sprintf("key retrieval for %s", value), 3)
		return nil, fmt.Errorf("%s experienced error while getting value: %w", c, err)
	}
	return key, nil
}

func getBucket(tx *bbolt.Tx, path [][]byte, mustExist bool) (*bbolt.Bucket, error) {
	bkt := tx.Bucket([]byte(rootBucket))
	if bkt == nil && mustExist {
		return nil, newErrAccess(fmt.Sprintf("%s in %s", path[0], path))
	} else if bkt == nil {
		return nil, nil
	}

	for _, p := range path {
		bkt = bkt.Bucket(p)
		if bkt == nil && mustExist {
			return nil, newErrAccess(fmt.Sprintf("%s in %s", p, path))
		} else if bkt == nil {
			return nil, nil
		}
	}

	return bkt, nil
}

// getFirstKeyAt returns the first key at the given path.
//
// If mustExist is true, an error will be returned if the key could not be found.
func getFirstKeyAt(db *bbolt.DB, path [][]byte, mustExist bool) ([]byte, error) {
	if db == nil {
		c := withCallerInfo(fmt.Sprintf("first key retrieval for %s", path), 3)
		return nil, fmt.Errorf("%s received nil db", c)
	}

	var key []byte

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		} else if bkt == nil {
			return nil
		}

		c := bkt.Cursor()
		key, _ = c.First()

		if key == nil && mustExist {
			return newErrLocate(fmt.Sprintf("first key at %#v", path))
		}

		return nil
	})

	if err != nil {
		c := withCallerInfo(fmt.Sprintf("first key retrieval for %s", path), 3)
		return nil, fmt.Errorf("%s experienced error while scanning keys: %w", c, err)
	}

	return key, nil
}

func valuesAt(db *bbolt.DB, path [][]byte, mustExist bool, buffer chan []byte, dbWrap dbWrapper) error {
	if db == nil {
		c := withCallerInfo(fmt.Sprintf("value iteration at %s", path), 3)
		return fmt.Errorf("%s received nil db", c)
	} else if buffer == nil {
		c := withCallerInfo(fmt.Sprintf("value iteration at %s", path), 3)
		return fmt.Errorf("%s received nil channel", c)
	}

	var values [][]byte

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		} else if bkt == nil {
			return nil
		}

		c := bkt.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			timer := time.NewTimer(dbWrap.bufferTimeout)
			select {
			case buffer <- k:
				timer.Stop()
			case <-timer.C:
				err := newErrTimeout("value iteration", "waiting to send to buffer")
				logMutex.Lock()
				dbWrap.logger.Err(err).Msg("")
				logMutex.Unlock()
				return err
			}
			values = append(values, v)
		}

		return nil
	})

	if err != nil {
		c := withCallerInfo(fmt.Sprintf("value iteration at %s", path), 3)
		return fmt.Errorf("%s experienced error while scanning db: %w", c, err)
	}

	return nil
}

func keysAt(db *bbolt.DB, path [][]byte, mustExist bool, buffer chan []byte, dbWrap dbWrapper) error {
	if db == nil {
		c := withCallerInfo(fmt.Sprintf("key iteration at %s", path), 3)
		return fmt.Errorf("%s received nil db", c)
	} else if buffer == nil {
		c := withCallerInfo(fmt.Sprintf("key iteration at %s", path), 3)
		return fmt.Errorf("%s received nil channel", c)
	}

	defer close(buffer)

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		} else if bkt == nil {
			return nil
		}

		c := bkt.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				continue
			}

			timer := time.NewTimer(dbWrap.bufferTimeout)
			select {
			case buffer <- k:
				timer.Stop()
			case <-timer.C:
				err := newErrTimeout("quickbolt key retrieval", "waiting to send to buffer")
				logMutex.Lock()
				dbWrap.logger.Err(err).Msg("")
				logMutex.Unlock()
				return err
			}
		}
		return nil
	})

	if err != nil {
		c := withCallerInfo(fmt.Sprintf("key iteration at %s", path), 3)
		return fmt.Errorf("%s experienced error while scanning keys: %w", c, err)
	}
	return nil
}

func entriesAt(db *bbolt.DB, path [][]byte, mustExist bool, buffer chan [2][]byte, dbWrap dbWrapper) error {
	if db == nil {
		c := withCallerInfo(fmt.Sprintf("key-value iteration at %s", path), 3)
		return fmt.Errorf("%s received nil db", c)
	} else if buffer == nil {
		c := withCallerInfo(fmt.Sprintf("key-value iteration at %s", path), 3)
		return fmt.Errorf("%s received nil channel", c)
	}

	defer close(buffer)

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		} else if bkt == nil {
			return nil
		}

		c := bkt.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				continue
			}

			timer := time.NewTimer(dbWrap.bufferTimeout)
			select {
			case buffer <- [2][]byte{k, v}:
				timer.Stop()
			case <-timer.C:
				err := newErrTimeout("quickbolt key scanning", "waiting to send to buffer")
				logMutex.Lock()
				dbWrap.logger.Err(err).Msg("")
				logMutex.Unlock()
				return err
			}
		}
		return nil
	})

	if err != nil {
		c := withCallerInfo(fmt.Sprintf("key-value iteration at %s", path), 3)
		return fmt.Errorf("%s experienced error while scanning keys: %w", c, err)
	}
	return nil
}

func bucketsAt(db *bbolt.DB, path [][]byte, mustExist bool, buffer chan []byte, dbWrap dbWrapper) error {
	if db == nil {
		c := withCallerInfo(fmt.Sprintf("bucket iteration at %s", path), 3)
		return fmt.Errorf("%s received nil db", c)
	} else if buffer == nil {
		c := withCallerInfo(fmt.Sprintf("bucket iteration at %s", path), 3)
		return fmt.Errorf("%s received nil channel", c)
	}

	defer close(buffer)

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		} else if bkt == nil {
			return nil
		}

		c := bkt.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v != nil {
				continue
			}

			timer := time.NewTimer(dbWrap.bufferTimeout)
			select {
			case buffer <- k:
				timer.Stop()
			case <-timer.C:
				err := newErrTimeout("quickbolt key scanning", "waiting to send to buffer")
				logMutex.Lock()
				dbWrap.logger.Err(err).Msg("")
				logMutex.Unlock()
				return err
			}
		}
		return nil
	})

	if err != nil {
		c := withCallerInfo(fmt.Sprintf("bucket iteration at %s", path), 3)
		return fmt.Errorf("%s experienced error while scanning buckets: %w", c, err)
	}
	return nil
}
