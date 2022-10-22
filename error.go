package quickbolt

import "fmt"

var (
	ErrNotLocate = errNotLocate() // "could not locate"
	ErrAccess    = errAccess()    // "can not access"
)

func errNotLocate() error {
	return fmt.Errorf("could not locate")
}
func errAccess() error {
	return fmt.Errorf("can not access")
}
