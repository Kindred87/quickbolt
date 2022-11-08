package quickbolt

import "fmt"

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
