package quickbolt

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"go.etcd.io/bbolt"
)

type DB interface {
	// Upsert adds the key-value pair to the db at the given path.  If the key is already present in the
	// db, then the sum of the existing and given values via add() will be inserted instead.
	//
	// Buckets in the path are created if they do not already exist.
	Upsert(key []byte, val []byte, bucketPath []string, add func(a, b []byte) ([]byte, error)) error
	// Insert adds the given key-value pair to the db at the given path.
	//
	// Buckets in the path are created if they do not already exist.
	Insert(key, value []byte, bucketPath []string) error
	// Delete removes the key-value pair in the db at the given path.
	Delete(key []byte, bucketPath []string) error
	// DeleteValues removes all key-value pairs in the db at the given path where the value
	// matches the one given.
	DeleteValues(value []byte, bucketPath []string) error
	// getValue returns the value paired with the given key.  The returned value will be nil
	// if the key could not be found.
	//
	// If mustExist is true, an error will be returned if the key could not
	// be found.
	GetValue(key []byte, bucketPath []string, mustExist bool) ([]byte, error)
	// getFirstKeyAt returns the first key at the given path.
	//
	// If mustExist is true, an error will be returned if the key could not
	// be found.
	GetFirstKeyAt(bucketPath []string, mustExist bool) ([]byte, error)
	// ValuesAt returns the values for all the keys at the given path.
	ValuesAt(bucketPath []string, mustExist bool, buffer chan []byte) error
	// KeysAt returns the keys at the given path.
	KeysAt(bucketPath []string, mustExist bool, buffer chan []byte) error
	// EntriesAt returns the key-value pairs at the given path.
	EntriesAt(bucketPath []string, mustExist bool, buffer chan [2][]byte) error
	// BucketsAt returns the buckets at the given path.
	BucketsAt(bucketPath []string, mustExist bool, buffer chan []byte) error
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
	// AddLog provides a writer interface through which quickbolt will log
	// buffer related errors via zerolog.
	//
	// The default log output is os.Stdout.
	AddLog(io.Writer)
	// SetBufferTimeout sets the timeout for buffer operations.
	//
	// The default is 1 second.
	SetBufferTimeout(time.Duration)
}

// Create generates a database with the given filename and returns a DB
// interface encapsulating the database.
//
// If the dir parameter is provided, the database will be created there.
// Otherwise, the database will be created in the executable's directory.
//
// If the database file already exists, it will be deleted and replaced
// with a new one.
func Create(filename string, dir ...string) (DB, error) {
	var dbPath string

	if dir == nil {
		exec, err := execDir()
		if err != nil {
			return nil, fmt.Errorf("error while getting executable dir: %w", err)
		}

		dbPath = filepath.Join(exec, filename)
	} else if len(dir) >= 0 && filepath.Ext(dir[0]) != "" {
		dbPath = filepath.Dir(dir[0])
	} else if len(dir) >= 0 {
		dbPath = dir[0]
	}

	os.Remove(dbPath)

	d, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("error while opening db at %s: %w", filename, err)
	}

	db := dbWrapper{db: d, bufferTimeout: defaultBufferTimeout}
	db.logger = zerolog.New(os.Stdout)

	return &db, nil
}

// dbWrapper is an encapsulation of a BBolt DB that implements the DB interface.
type dbWrapper struct {
	db            *bbolt.DB
	logger        zerolog.Logger
	bufferTimeout time.Duration
}

func (d dbWrapper) Upsert(key []byte, val []byte, path []string, add func(a, b []byte) ([]byte, error)) error {
	return upsert(d.db, key, val, path, add)
}

func (d dbWrapper) Insert(key, value []byte, path []string) error {
	return insert(d.db, key, value, path)
}

func (d dbWrapper) Delete(key []byte, path []string) error {
	return delete(d.db, key, path)
}

func (d dbWrapper) DeleteValues(value []byte, path []string) error {
	return deleteValues(d.db, value, path)
}

func (d dbWrapper) GetValue(key []byte, path []string, mustExist bool) ([]byte, error) {
	return getValue(d.db, key, path, mustExist)
}

func (d dbWrapper) GetFirstKeyAt(path []string, mustExist bool) ([]byte, error) {
	return getFirstKeyAt(d.db, path, mustExist)
}

func (d dbWrapper) ValuesAt(path []string, mustExist bool, buffer chan []byte) error {
	return valuesAt(d.db, path, mustExist, buffer, d)
}

func (d dbWrapper) KeysAt(path []string, mustExist bool, buffer chan []byte) error {
	return keysAt(d.db, path, mustExist, buffer, d)
}

func (d dbWrapper) EntriesAt(path []string, mustExist bool, buffer chan [2][]byte) error {
	return entriesAt(d.db, path, mustExist, buffer, d)
}

func (d dbWrapper) BucketsAt(path []string, mustExist bool, buffer chan []byte) error {
	return bucketsAt(d.db, path, mustExist, buffer, d)
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
