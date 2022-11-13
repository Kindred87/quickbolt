package quickbolt

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

func dbPath(filename string, dir ...string) (string, error) {
	if filename == "" {
		return "", fmt.Errorf("filename is empty")
	}
	var dbPath string

	if dir == nil {
		exec, err := execDir()
		if err != nil {
			return "", fmt.Errorf("error while getting executable dir: %w", err)
		}

		dbPath = filepath.Join(exec, filename)
	} else if len(dir) >= 0 && filepath.Ext(dir[0]) != "" {
		dbPath = filepath.Join(filepath.Dir(dir[0]), filename)
	} else if len(dir) >= 0 {
		dbPath = filepath.Join(filename, dir[0])
	}

	return dbPath, nil
}

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

func removeFile(db *bbolt.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	path := db.Path()

	if err := closeDB(db); err != nil {
		return fmt.Errorf("error while closing db: %w", err)
	}

	return os.Remove(path)
}
