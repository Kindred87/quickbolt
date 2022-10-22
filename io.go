package quickbolt

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

func execDir() (string, error) {
	exec, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("error while fetching executable path: %w", err)
	}

	return filepath.Dir(exec), nil
}

func closeDB(db *bbolt.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	return db.Close()
}

func remove(db *bbolt.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	path := db.Path()

	if err := closeDB(db); err != nil {
		return fmt.Errorf("error while closing db: %w", err)
	}

	return os.Remove(path)
}
