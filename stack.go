package quickbolt

import (
	"fmt"
	"runtime"
)

// getCallerInfo returns a string describing the file and line number of the caller for code calling this function.
// The string is formatted as:
//   - "<task> called at line <line number> in <file>"
//
// If error occurs, the returned string will be formatted as:
//   - "<task>"
func getCallerInfo(task string) string {
	_, file, line, ok := runtime.Caller(2)

	if !ok {
		return task
	}

	if len(task) > 0 {
		return fmt.Sprintf("%s called at line %d in %s", task, line, file)
	}

	return fmt.Sprintf("%s on line %d", file, line)
}
