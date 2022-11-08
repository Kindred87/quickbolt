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
		return nil, fmt.Errorf("db is nil")
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
		return nil, fmt.Errorf("error while reading value paired with key %s: %w", string(key), err)
	}
	return value, nil
}

func getKey(db *bbolt.DB, value []byte, path [][]byte, mustExist bool) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
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
		return nil, fmt.Errorf("error while reading value paired with key %s: %w", string(key), err)
	}
	return key, nil
}

func getBucket(tx *bbolt.Tx, path [][]byte, mustExist bool) (*bbolt.Bucket, error) {
	bkt := tx.Bucket([]byte(rootBucket))
	if bkt == nil && mustExist {
		return nil, newErrAccess(fmt.Sprintf("%s in %#v", path[0], path))
	} else if bkt == nil {
		return nil, nil
	}

	for _, p := range path {
		bkt = bkt.Bucket(p)
		if bkt == nil && mustExist {
			return nil, newErrAccess(fmt.Sprintf("%s in %#v", p, path))
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
		return nil, fmt.Errorf("error while scanning keys at %#v: %w", path, err)
	}

	return key, nil
}

func valuesAt(db *bbolt.DB, path [][]byte, mustExist bool, buffer chan []byte, dbWrap dbWrapper) error {
	if db == nil {
		return fmt.Errorf("db is nil")
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
				err := newErrTimeout("quickbolt value retrieval", "waiting to send to buffer")
				dbWrap.logger.Err(err).Msg("")
				return err
			}
			values = append(values, v)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error while scanning db: %w", err)
	}

	return nil
}

func keysAt(db *bbolt.DB, path [][]byte, mustExist bool, buffer chan []byte, dbWrap dbWrapper) error {
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
				dbWrap.logger.Err(err).Msg("")
				return err
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error while scanning keys at %#v: %w", path, err)
	}
	return nil
}

func entriesAt(db *bbolt.DB, path [][]byte, mustExist bool, buffer chan [2][]byte, dbWrap dbWrapper) error {
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
				dbWrap.logger.Err(err).Msg("")
				return err
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error while scanning keys at %#v: %w", path, err)
	}
	return nil
}

func bucketsAt(db *bbolt.DB, path [][]byte, mustExist bool, buffer chan []byte, dbWrap dbWrapper) error {
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
				dbWrap.logger.Err(err).Msg("")
				return err
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error while scanning buckets at %#v: %w", path, err)
	}
	return nil
}
