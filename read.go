package quickbolt

import (
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"go.etcd.io/bbolt"
)

// getValue returns the value paired with the given key.  The returned value will be nil
// if the key could not be found.
//
// If mustExist is true, an error will be returned if the key could not
// be found.
func getValue(db *bbolt.DB, key []byte, path []string, mustExist bool) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}

	var value []byte

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		value = bkt.Get(key)
		if value == nil && mustExist {
			return fmt.Errorf("could not locate key %s at %#v", string(key), path)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error while reading value paired with key %s: %w", string(key), err)
	}
	return value, nil
}

func getBucket(tx *bbolt.Tx, path []string, mustExist bool) (*bbolt.Bucket, error) {
	bkt := tx.Bucket([]byte(rootBucket))
	if bkt == nil && mustExist {
		return nil, fmt.Errorf("%w %s in %#v", ErrAccess, path[0], path)
	} else if bkt == nil {
		return nil, nil
	}

	for _, p := range path {
		bkt = bkt.Bucket([]byte(p))
		if bkt == nil && mustExist {
			return nil, fmt.Errorf("%w %s in %#v", ErrAccess, p, path)
		} else if bkt == nil {
			return nil, nil
		}
	}

	return bkt, nil
}

// getFirstKeyAt returns the first key at the given path.
//
// If mustExist is true, an error will be returned if the key could not
// be found.
func getFirstKeyAt(db *bbolt.DB, path []string, mustExist bool) ([]byte, error) {
	var key []byte

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		c := bkt.Cursor()
		key, _ = c.First()

		if key == nil && mustExist {
			return fmt.Errorf("could not locate first key at %#v", path)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error while scanning keys at %#v: %w", path, err)
	}

	return key, nil
}

func valuesAt(db *bbolt.DB, path []string, mustExist bool) ([][]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}

	var values [][]byte

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		c := bkt.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			values = append(values, v)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error while scanning db: %w", err)
	}

	return values, nil
}

func keysAt(db *bbolt.DB, path []string, mustExist bool, buffer chan []byte, logger zerolog.Logger) error {
	defer close(buffer)

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		c := bkt.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			select {
			case buffer <- k:
			case <-time.After(time.Second * 5):
				err := fmt.Errorf("quickbolt key scanning timed out while waiting to send to buffer")
				logger.Err(err).Msg("")
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

func entriesAt(db *bbolt.DB, path []string, mustExist bool, buffer chan [2][]byte, logger zerolog.Logger) error {
	defer close(buffer)

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		c := bkt.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			select {
			case buffer <- [2][]byte{k, v}:
			case <-time.After(time.Second * 5):
				err := fmt.Errorf("quickbolt key scanning timed out while waiting to send to buffer")
				logger.Err(err).Msg("")
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

func bucketsAt(db *bbolt.DB, path []string, mustExist bool, buffer chan []byte, logger zerolog.Logger) error {
	defer close(buffer)

	err := db.View(func(tx *bbolt.Tx) error {
		bkt, err := getBucket(tx, path, mustExist)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		c := bkt.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v != nil {
				continue
			}
			select {
			case buffer <- k:
			case <-time.After(time.Second * 5):
				err := fmt.Errorf("quickbolt key scanning timed out while waiting to send to buffer")
				logger.Err(err).Msg("")
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
