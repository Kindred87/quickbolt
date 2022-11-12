package quickbolt

import (
	"fmt"
	"runtime"
)

// getCallerInfo returns a string describing the file and line number of the caller for code calling this function.
// The string is formatted as: "<file> on line <line number> ", or "<prepend value> <file> on line <line number> " if prepend is not empty.
//
// Note that returned non-empty strings are appended with an empty space.
// This allows the caller to safely prepend their message with %s without concern for whitespace.
//
// The returned string will be empty if an error occurred while resolving the call stack.
func getCallerInfo(prepend string) string {
	_, file, line, ok := runtime.Caller(2)

	if !ok {
		return ""
	}

	if prepend != "" {
		return fmt.Sprintf("%s %s on line %d ", prepend, file, line)
	}

	return fmt.Sprintf("%s on line %d ", file, line)
}
