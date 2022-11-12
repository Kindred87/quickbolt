package quickbolt

import (
	"fmt"
	"runtime"
)

// getCallerInfo returns a string describing the file and line number of the caller for code calling this function.
// The string is formatted as one of the following, depending on whether task is empty:
//   - "<file> on line <line number> "
//   - "<task> called at line <line number> in <file> "
//
// If task is not empty and an error occurs, the returned string will be formatted as:
//   - "<task> "
//
// Note that returned non-empty strings are appended with an empty space.
// This allows the caller to safely prepend their message with %s without concern for whitespace.
//
// The returned string will be empty if an error occurred while resolving the call stack and task is empty.
func getCallerInfo(task ...string) string {
	_, file, line, ok := runtime.Caller(2)

	if !ok && len(task) > 0 {
		return task[0] + " "
	} else if !ok {
		return ""
	}

	if len(task) > 0 {
		return fmt.Sprintf("%s called at line %d in %s ", task[0], line, file)
	}

	return fmt.Sprintf("%s on line %d ", file, line)
}
