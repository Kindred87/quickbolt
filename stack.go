package quickbolt

import (
	"fmt"
	"runtime"
)

// getCallerInfo returns a string describing the file and line number of the caller for code calling this function.
// The string is formatted as one of the following, depending on whether task contains a value:
//   - "<file> on line <line number> "
//   - "<task> called at line <line number> in <file> "
//
// Note that returned non-empty strings are appended with an empty space.
// This allows the caller to safely prepend their message with %s without concern for whitespace.
//
// The returned string will be empty if an error occurred while resolving the call stack.
func getCallerInfo(task ...string) string {
	_, file, line, ok := runtime.Caller(2)

	if !ok {
		return ""
	}

	if len(task) > 0 {
		return fmt.Sprintf("%s called at line %d in %s ", task[0], line, file)
	}

	return fmt.Sprintf("%s on line %d ", file, line)
}
