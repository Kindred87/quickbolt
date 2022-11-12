package quickbolt

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"go.etcd.io/bbolt"
)

type DB interface {
	// Upsert writes the key-value pair to the db at the given path.
	// If the key is already present in the db, then the sum of the existing and given values via add() will be inserted instead.
	//
	// Key and value must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	//
	// Buckets in the path are created if they do not already exist.
	Upsert(key, value, bucketPath any, add func(a, b []byte) ([]byte, error)) error
	// Insert writes the given key-value pair to the db at the given path.
	//
	// Key and value must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	//
	// Buckets in the path are created if they do not already exist.
	Insert(key, value, bucketPath any) error
	// InsertValue writes the given value to the db at the given path using an automatically generated key.
	// Note that the key will be an endian-specific uint64 value.  You may use the PerEndian helper function to assist your byte conversions.
	//
	// Value must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	//
	// Buckets in the path are created if they do not already exist.
	InsertValue(value, bucketPath any) error
	// InsertBucket creates a bucket of the given key in the db at the given path.
	//
	// Key must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	//
	// Buckets in the path are created uf they do not already exist.
	InsertBucket(key, bucketPath any) error
	// Delete removes the key-value pair in the db at the given path.
	//
	// Key must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	Delete(key, bucketPath any) error
	// DeleteValues removes all key-value pairs in the db at the given path where the value matches the one given.
	//
	// Value must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	DeleteValues(value, bucketPath any) error
	// getValue returns the value paired with the given key.
	// The returned value will be nil if the key could not be found.
	//
	// Key must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	//
	// If mustExist is true, an error will be returned if the key could not be found.
	GetValue(key, bucketPath any, mustExist bool) ([]byte, error)
	// getKey returns the key paired with the given value.
	// The returned value will be nil if the value could not be found.
	//
	// Value must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	//
	// If mustExist is true, an error will be returned if the value could not be found.
	GetKey(value, bucketPath any, mustExist bool) ([]byte, error)
	// getFirstKeyAt returns the first key at the given path.
	//
	// BucketPath must be of type []string or [][]byte.
	//
	// If mustExist is true, an error will be returned if the key could not be found.
	GetFirstKeyAt(bucketPath any, mustExist bool) ([]byte, error)
	// ValuesAt returns the values for all the keys at the given path.
	//
	// Key and val must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	ValuesAt(bucketPath any, mustExist bool, buffer chan []byte) error
	// KeysAt returns the keys at the given path.
	//
	// Key and val must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	KeysAt(bucketPath any, mustExist bool, buffer chan []byte) error
	// EntriesAt returns the key-value pairs at the given path.
	//
	// Key and val must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	EntriesAt(bucketPath any, mustExist bool, buffer chan [2][]byte) error
	// BucketsAt returns the buckets at the given path.
	//
	// Key and val must be of type []byte, string, int, or uint64.
	//
	// BucketPath must be of type []string or [][]byte.
	BucketsAt(bucketPath any, mustExist bool, buffer chan []byte) error
	// RunView executes a custom view func on the database.
	//
	// Use the RootBucket method to get the database's root bucket.
	RunView(func(tx *bbolt.Tx) error) error
	// RunUpdate executes a custom update func on the database.
	//
	// Use the RootBucket method to get the database's root bucket.
	RunUpdate(func(tx *bbolt.Tx) error) error
	// Close closes the database.
	Close() error
	// RemoveFile deletes the database.
	RemoveFile() error
	// Size returns the Size struct for the database, used to get the file size of the db.
	Size() Size
	// Path returns the path of the database file.
	Path() string
	// RootBucket returns the root bucket's identifier.
	RootBucket() []byte
	// AddLog provides a writer interface through which quickbolt will log buffer related errors via zerolog.
	//
	// The default log output is os.Stdout.
	AddLog(io.Writer)
	// SetBufferTimeout sets the timeout for buffer operations.
	//
	// The default is 1 second.
	SetBufferTimeout(time.Duration)
}

// Create generates a database with the given filename and returns a DB interface encapsulating the database.
//
// If the dir parameter is provided, the database will be created there.
// Otherwise, the database will be created in the executable's directory.
//
// If the database file already exists, it will be deleted and replaced
// with a new one.
func Create(filename string, dir ...string) (DB, error) {
	path, err := dbPath(filename, dir...)
	if err != nil {
		return nil, fmt.Errorf("error while resolving database path: %w", err)
	}

	os.Remove(path)

	db, err := new(path)
	if err != nil {
		return nil, fmt.Errorf("error while opening database: %w", err)
	}

	return db, nil
}

func new(path string) (DB, error) {
	d, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("error while opening db at %s: %w", path, err)
	}

	db := dbWrapper{db: d, bufferTimeout: defaultBufferTimeout}
	db.logger = zerolog.New(os.Stdout)

	return &db, nil
}

// Open opens a database with the given filename and returns a DB interface encapsulating the database.
//
// If the dir parameter is provided, the database will be opened there.
// Otherwise, the database will be opened in the executable's directory.
//
// The database will be created if it does not already exist.
func Open(filename string, dir ...string) (DB, error) {
	path, err := dbPath(filename, dir...)
	if err != nil {
		return nil, fmt.Errorf("error while resolving database path: %w", err)
	}

	db, err := new(path)
	if err != nil {
		return nil, fmt.Errorf("error while opening database: %w", err)
	}

	return db, nil
}

// dbWrapper is an encapsulation of a BBolt DB that implements the DB interface.
type dbWrapper struct {
	db            *bbolt.DB
	logger        zerolog.Logger
	bufferTimeout time.Duration
}

func (d dbWrapper) Upsert(key, val, path any, add func(a, b []byte) ([]byte, error)) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo("value upsert", 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	k, err := resolveRecord(key)
	if err != nil {
		c := withCallerInfo("value upsert", 2)
		return fmt.Errorf("%s %w", c, newErrRecordResolution("key", key))
	}

	v, err := resolveRecord(val)
	if err != nil {
		c := withCallerInfo("value upsert", 2)
		return fmt.Errorf("%s %w", c, newErrRecordResolution("value", val))
	}

	return upsert(d.db, k, v, p, add)
}

func (d dbWrapper) Insert(key, val, path any) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo("key-value insertion", 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	k, err := resolveRecord(key)
	if err != nil {
		c := withCallerInfo("key-value insertion", 2)
		return fmt.Errorf("%s %w", c, newErrRecordResolution("key", key))
	}

	v, err := resolveRecord(val)
	if err != nil {
		c := withCallerInfo("key-value insertion", 2)
		return fmt.Errorf("%s %w", c, newErrRecordResolution("value", val))
	}

	return insert(d.db, k, v, p)
}

func (d dbWrapper) InsertValue(val, path any) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo("value insertion", 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	v, err := resolveRecord(val)
	if err != nil {
		c := withCallerInfo("value insertion", 2)
		return fmt.Errorf("%s %w", c, newErrRecordResolution("value", val))
	}

	return insertValue(d.db, v, p)
}

func (d dbWrapper) InsertBucket(key, path any) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo("bucket insertion", 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	k, err := resolveRecord(key)
	if err != nil {
		c := withCallerInfo("bucket insertion", 2)
		return fmt.Errorf("%s %w", c, newErrRecordResolution("key", key))
	}

	return insertBucket(d.db, k, p)
}

func (d dbWrapper) Delete(key, path any) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo("key-value deletion", 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	k, err := resolveRecord(key)
	if err != nil {
		c := withCallerInfo("key-value deletion", 2)
		return fmt.Errorf("%s %w", c, newErrRecordResolution("key", key))
	}

	return delete(d.db, k, p)
}

func (d dbWrapper) DeleteValues(val, path any) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo("value deletion", 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	v, err := resolveRecord(val)
	if err != nil {
		c := withCallerInfo("value deletion", 2)
		return fmt.Errorf("%s %w", c, newErrRecordResolution("value", val))
	}

	return deleteValues(d.db, v, p)
}

func (d dbWrapper) GetValue(key, path any, mustExist bool) ([]byte, error) {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo("value retrieval", 2)
		return nil, fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	k, err := resolveRecord(key)
	if err != nil {
		c := withCallerInfo("value retrieval", 2)
		return nil, fmt.Errorf("%s %w", c, newErrRecordResolution("key", key))
	}

	return getValue(d.db, k, p, mustExist)
}

func (d dbWrapper) GetKey(val, path any, mustExist bool) ([]byte, error) {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo("key retrieval", 2)
		return nil, fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	v, err := resolveRecord(val)
	if err != nil {
		c := withCallerInfo("key retrieval", 2)
		return nil, fmt.Errorf("%s %w", c, newErrRecordResolution("value", val))
	}

	return getKey(d.db, v, p, mustExist)
}

func (d dbWrapper) GetFirstKeyAt(path any, mustExist bool) ([]byte, error) {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo(fmt.Sprintf("first key retrieval in %s", path), 2)
		return nil, fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	return getFirstKeyAt(d.db, p, mustExist)
}

func (d dbWrapper) ValuesAt(path any, mustExist bool, buffer chan []byte) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo(fmt.Sprintf("value iteration in %s", path), 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	return valuesAt(d.db, p, mustExist, buffer, d)
}

func (d dbWrapper) KeysAt(path any, mustExist bool, buffer chan []byte) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo(fmt.Sprintf("key iteration in %s", path), 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	return keysAt(d.db, p, mustExist, buffer, d)
}

func (d dbWrapper) EntriesAt(path any, mustExist bool, buffer chan [2][]byte) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo(fmt.Sprintf("key-value iteration in %s", path), 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	return entriesAt(d.db, p, mustExist, buffer, d)
}

func (d dbWrapper) BucketsAt(path any, mustExist bool, buffer chan []byte) error {
	p, err := resolveBucketPath(path)
	if err != nil {
		c := withCallerInfo(fmt.Sprintf("bucket iteration in %s", path), 2)
		return fmt.Errorf("%s experienced %w", c, newErrBucketPathResolution("error"))
	}

	return bucketsAt(d.db, p, mustExist, buffer, d)
}

func (d dbWrapper) RunView(f func(tx *bbolt.Tx) error) error {
	return d.db.View(f)
}

func (d dbWrapper) RunUpdate(f func(tx *bbolt.Tx) error) error {
	return d.db.Update(f)
}

func (d dbWrapper) Close() error {
	return closeDB(d.db)
}

func (d dbWrapper) RemoveFile() error {
	return removeFile(d.db)
}

func (d dbWrapper) Size() Size {
	if d.db == nil {
		return sizeStore{}
	}

	stats, err := os.Stat(d.db.Path())
	if err != nil {
		return sizeStore{}
	}
	return newSizeStore(int(stats.Size() / 1048576))
}

func (d dbWrapper) Path() string {
	return d.db.Path()
}

func (d dbWrapper) RootBucket() []byte {
	return []byte(rootBucket)
}

func (d *dbWrapper) AddLog(w io.Writer) {
	d.logger = zerolog.New(w)
}

func (d *dbWrapper) SetBufferTimeout(t time.Duration) {
	d.bufferTimeout = t
}
