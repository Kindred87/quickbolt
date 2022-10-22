package quickbolt

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

type DB interface {
	// Upsert adds the key-value pair to the db at the given path.  If the key is already present in the
	// db, then the sum of the existing and given values will be added to the db instead.
	Upsert(key []byte, val []byte, path []string, addFunc func(a, b []byte) ([]byte, error)) error
	// Insert adds the given key-value pair to the db at the given path.
	Insert(key, value []byte, path []string) error
	// Delete removes the key-value pair in the db at the given path.
	Delete(key []byte, path []string) error
	// DeleteValues removes all key-value pairs in the db at the given path where the value
	// matches the one given.
	DeleteValues(value []byte, path []string) error
	// getValue returns the value paired with the given key.  The returned value will be nil
	// if the key could not be found.
	//
	// If mustExist is true, an error will be returned if the key could not
	// be found.
	GetValue(key []byte, path []string, mustExist bool) ([]byte, error)
	// getFirstKeyAt returns the first key at the given path.
	//
	// If mustExist is true, an error will be returned if the key could not
	// be found.
	GetFirstKeyAt(path []string, mustExist bool) ([]byte, error)
	// ValuesAt returns the values for all the keys at the given path.
	ValuesAt(path []string, mustExist bool) ([][]byte, error)
	// KeysAt returns the keys at the given path.
	KeysAt(path []string, mustExist bool, buffer chan []byte) error
	// EntriesAt returns the key-value pairs at the given path.
	EntriesAt(path []string, mustExist bool, buffer chan [2][]byte) error
	// BucketsAt returns the buckets at the given path.
	BucketsAt(path []string, mustExist bool, buffer chan []byte) error
	// Close closes the database.
	Close() error
	// RemoveFile deletes the database.
	RemoveFile() error
	// Size returns the Size struct for the database, used to get the file size of the db.
	Size() Size
	// Path returns the path of the open database file.
	Path() string
}

// Create generates a database with the given filename and returns a DB
// interface encapsulating the database.
func Create(filename string) (DB, error) {
	dir, err := execDir()
	if err != nil {
		return dbFile{}, fmt.Errorf("error while getting executable dir: %w", err)
	}

	dbPath := filepath.Join(dir, filename)

	os.Remove(dbPath)

	d, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return dbFile{}, fmt.Errorf("error while opening db at %s: %w", filename, err)
	}

	db := dbFile{db: d}
	return db, nil
}

// dbFile is an encapsulation of a BBolt DB that implements the DB interface.
type dbFile struct {
	db *bbolt.DB
}

func (d dbFile) Upsert(key []byte, val []byte, path []string, addFunc func(a, b []byte) ([]byte, error)) error {
	return upsert(d.db, key, val, path, addFunc)
}

func (d dbFile) Insert(key, value []byte, path []string) error {
	return insert(d.db, key, value, path)
}

func (d dbFile) Delete(key []byte, path []string) error {
	return delete(d.db, key, path)
}

func (d dbFile) DeleteValues(value []byte, path []string) error {
	return deleteValues(d.db, value, path)
}

func (d dbFile) GetValue(key []byte, path []string, mustExist bool) ([]byte, error) {
	return getValue(d.db, key, path, mustExist)
}

func (d dbFile) GetFirstKeyAt(path []string, mustExist bool) ([]byte, error) {
	return getFirstKeyAt(d.db, path, mustExist)
}

func (d dbFile) ValuesAt(path []string, mustExist bool) ([][]byte, error) {
	return valuesAt(d.db, path, mustExist)
}

func (d dbFile) KeysAt(path []string, mustExist bool, buffer chan []byte) error {
	return keysAt(d.db, path, mustExist, buffer)
}

func (d dbFile) EntriesAt(path []string, mustExist bool, buffer chan [2][]byte) error {
	return entriesAt(d.db, path, mustExist, buffer)
}

func (d dbFile) BucketsAt(path []string, mustExist bool, buffer chan []byte) error {
	return bucketsAt(d.db, path, mustExist, buffer)
}

func (d dbFile) Close() error {
	return closeDB(d.db)
}

func (d dbFile) RemoveFile() error {
	return removeFile(d.db)
}

func (d dbFile) Size() Size {
	if d.db == nil {
		return sizeStore{}
	}

	stats, err := os.Stat(d.db.Path())
	if err != nil {
		return sizeStore{}
	}
	return newSizeStore(int(stats.Size() / 1048576))
}

func (d dbFile) Path() string {
	return d.db.Path()
}
