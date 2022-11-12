package quickbolt

import (
	"fmt"
	"runtime"
)

// withCallerInfo returns a string describing the file and line number of the caller at the given offset.
//
// An offset value of 2 will return the caller of the function calling this function.
// An offset value of 3 will return the caller of the caller of the function calling this function.
//
// The returned string is formatted as:
//   - "<task> called at line <line number> in <file>"
//
// If task is empty, the returned string will be formatted as:
//   - "<file> on line <line number>"
//
// If error occurs, the returned string will be formatted as:
//   - "<task>"
func withCallerInfo(task string, offset int) string {
	_, file, line, ok := runtime.Caller(offset)

	if !ok {
		return task
	}

	if task != "" {
		return fmt.Sprintf("%s called at line %d in %s", task, line, file)
	}

	return fmt.Sprintf("%s on line %d", file, line)
}
