package quickbolt

import (
	"fmt"
	"strconv"
)

// resolveBucketPath returns a [] byte slice representing a bucket path.
//
// The following types are supported: []string, [][]byte
func resolveBucketPath(p interface{}) ([][]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("path is nil")
	}

	var resolved [][]byte

	switch path := p.(type) {
	case []string:
		for _, s := range path {
			resolved = append(resolved, []byte(s))
		}
	case [][]byte:
		resolved = append(resolved, path...)
	default:
		return nil, newErrUnsupportedType("path")
	}

	return resolved, nil
}

func resolveRecord(r interface{}) ([]byte, error) {
	if r == nil {
		return nil, fmt.Errorf("record is nil")
	}

	var resolved []byte

	switch record := r.(type) {
	case []byte:
		resolved = append(resolved, record...)
	case string:
		resolved = []byte(record)
	case int:
		resolved = []byte(strconv.Itoa(record))
	case uint64:
		t, err := toBytes(record)
		if err != nil {
			return nil, fmt.Errorf("error while resolving %d: %w", record, err)
		}
		resolved = t
	default:
		return nil, newErrUnsupportedType("record")
	}

	return resolved, nil
}
