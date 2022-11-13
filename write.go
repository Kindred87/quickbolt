package quickbolt

import (
	"fmt"
	"strconv"

	"go.etcd.io/bbolt"
	"golang.org/x/exp/slices"
)

// upsert adds the key-value pair to the db at the given path.
// If the key is already present in the db, then the sum of the existing and given values will be added to the db instead.
func upsert(db *bbolt.DB, key []byte, val []byte, path [][]byte, add func(a, b []byte) ([]byte, error)) error {
	err := db.Batch(func(tx *bbolt.Tx) error {
		bkt, err := getCreateBucket(tx, path)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		oldVal := bkt.Get(key)
		if oldVal != nil {
			new, err := add(oldVal, val)
			if err != nil {
				return fmt.Errorf("error while adding %s and %s: %w", oldVal, val, err)
			}
			val = new
		}

		err = bkt.Put(key, val)
		if err != nil {
			return fmt.Errorf("error while writing: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error while writing %s and %s to db: %w", string(key), string(val), err)
	}

	return nil
}

// getCreateBucket returns the bucket at the end of the given path, creating buckets if needed.
//
// The path will automatically be prepended with the db root.
func getCreateBucket(tx *bbolt.Tx, path [][]byte) (*bbolt.Bucket, error) {
	bkt, err := tx.CreateBucketIfNotExists([]byte(rootBucket))
	if err != nil {
		return nil, fmt.Errorf("error while accessing root bucket: %w", err)
	}

	for _, p := range path {
		bkt, err = bkt.CreateBucketIfNotExists(p)
		if err != nil {
			return nil, fmt.Errorf("error while accessing %s in %#v: %w", p, path, err)
		}
	}

	return bkt, nil
}

// insert adds the given key-value pair to the db at the given path.
func insert(db *bbolt.DB, key, value []byte, path [][]byte) error {
	err := db.Batch(func(tx *bbolt.Tx) error {
		bkt, err := getCreateBucket(tx, path)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		err = bkt.Put(key, value)
		if err != nil {
			return fmt.Errorf("error while writing: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error while writing %s and %s to db: %w", string(key), string(value), err)
	}

	return nil
}

// insertValue writes the given value to the db at the given path using an auto-generated key.
func insertValue(db *bbolt.DB, value []byte, path [][]byte) error {
	err := db.Batch(func(tx *bbolt.Tx) error {
		bkt, err := getCreateBucket(tx, path)
		if err != nil {
			c := withCallerInfo(fmt.Sprintf("value insertion for %v", value), 3)
			return fmt.Errorf("%s experienced error while navigating path: %w", c, err)
		}

		k, _ := bkt.NextSequence()

		err = bkt.Put([]byte(strconv.FormatUint(k, 10)), value)
		if err != nil {
			c := withCallerInfo(fmt.Sprintf("value insertion for %v", value), 3)
			return fmt.Errorf("%s experienced error while writing: %w", c, err)
		}

		return nil
	})

	if err != nil {
		c := withCallerInfo(fmt.Sprintf("value insertion for %v", value), 3)
		return fmt.Errorf("%s experienced error while writing %s to db: %w", c, string(value), err)
	}

	return nil
}

// insertBucket creates a bucket of the given key at the given path.
func insertBucket(db *bbolt.DB, key []byte, path [][]byte) error {
	err := db.Batch(func(tx *bbolt.Tx) error {
		bkt, err := getCreateBucket(tx, path)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		_, err = bkt.CreateBucket(key)
		if err != nil {
			return fmt.Errorf("error while creating bucket: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error while writing bucket %s to db: %w", string(key), err)
	}

	return nil
}

// delete removes the key-value pair in the db at the given path.
func delete(db *bbolt.DB, key []byte, path [][]byte) error {
	err := db.Batch(func(tx *bbolt.Tx) error {
		bkt, err := getCreateBucket(tx, path)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		return bkt.Delete(key)
	})

	if err != nil {
		return fmt.Errorf("error while deleting %s from db: %w", string(key), err)
	}

	return nil
}

// deleteValues removes all key-value pairs in the db at the given path where the value matches the one given.
func deleteValues(db *bbolt.DB, value []byte, path [][]byte) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	tx, err := db.Begin(true)
	if err != nil {
		return fmt.Errorf("error while initializing entry removal transaction: %w", err)
	}

	defer tx.Rollback()

	bkt, err := getCreateBucket(tx, path)
	if err != nil {
		return fmt.Errorf("error while navigating path: %w", err)
	}

	c := bkt.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {

		if slices.Equal(v, value) {
			if err := c.Delete(); err != nil {
				return fmt.Errorf("error while deleting key %s: %w", string(k), err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error while committing entry removals: %w", err)
	}

	return nil
}
