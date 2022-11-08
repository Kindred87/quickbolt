package quickbolt

import (
	"fmt"

	"go.etcd.io/bbolt"
	"golang.org/x/exp/slices"
)

// upsert adds the key-value pair to the db at the given path.
// If the key is already present in the db, then the sum of the existing and given values will be added to the db instead.
func upsert(db *bbolt.DB, key []byte, val []byte, path []string, add func(a, b []byte) ([]byte, error)) error {
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
		return fmt.Errorf("error while writing %#v and %#v to db: %w", key, val, err)
	}

	return nil
}

// getCreateBucket returns the bucket at the end of the given path, creating buckets if needed.
//
// The path will automatically be prepended with the db root.
func getCreateBucket(tx *bbolt.Tx, path []string) (*bbolt.Bucket, error) {
	bkt, err := tx.CreateBucketIfNotExists([]byte(rootBucket))
	if err != nil {
		return nil, fmt.Errorf("error while accessing root bucket: %w", err)
	}

	for _, p := range path {
		bkt, err = bkt.CreateBucketIfNotExists([]byte(p))
		if err != nil {
			return nil, fmt.Errorf("error while accessing %s in %#v: %w", p, path, err)
		}
	}

	return bkt, nil
}

// insert adds the given key-value pair to the db at the given path.
func insert(db *bbolt.DB, key, value []byte, path []string) error {
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
		return fmt.Errorf("error while writing %#v and %#v to db: %w", key, value, err)
	}

	return nil
}

// delete removes the key-value pair in the db at the given path.
func delete(db *bbolt.DB, key []byte, path []string) error {
	err := db.Batch(func(tx *bbolt.Tx) error {
		bkt, err := getCreateBucket(tx, path)
		if err != nil {
			return fmt.Errorf("error while navigating path: %w", err)
		}

		return bkt.Delete(key)
	})

	if err != nil {
		return fmt.Errorf("error while deleting %v from db: %w", key, err)
	}

	return nil
}

// deleteValues removes all key-value pairs in the db at the given path where the value matches the one given.
func deleteValues(db *bbolt.DB, value []byte, path []string) error {
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
				return fmt.Errorf("error while deleting key %#v: %w", k, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error while committing entry removals: %w", err)
	}

	return nil
}
