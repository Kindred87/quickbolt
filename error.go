package quickbolt

import (
	"fmt"
	"strings"
)

const (
	errLocateMsg               = "could not locate"
	errAccessMsg               = "could not access"
	errUnsupportedTypeMsg      = "is unsupported type"
	errTimeoutMsg              = "timed out while"
	errBucketPathResolutionMsg = "while resolving bucket path"
	errRecordResolutionMsg     = "could not resolve"
)

// "could not locate X"
type ErrLocate struct {
	What string
}

func (e ErrLocate) Error() string {
	return fmt.Sprintf("%s %s", errLocateMsg, e.What)
}

func (e ErrLocate) Is(target error) bool {
	return strings.HasPrefix(target.Error(), errLocateMsg)
}

// "could not locate" what
func newErrLocate(what string) error {
	return ErrLocate{What: what}
}

// "could not access X"
type ErrAccess struct {
	What string
}

func (e ErrAccess) Error() string {
	return fmt.Sprintf("%s %s", errAccessMsg, e.What)
}

func (e ErrAccess) Is(target error) bool {
	return strings.HasPrefix(target.Error(), errAccessMsg)
}

// "could not access" what
func newErrAccess(what string) error {
	return ErrAccess{What: what}
}

// "X is unsupported type"
type ErrUnsupportedType struct {
	What string
}

func (e ErrUnsupportedType) Error() string {
	return fmt.Sprintf("%s %s", e.What, errUnsupportedTypeMsg)
}

// what "is unsupported type"
func newErrUnsupportedType(what string) error {
	return ErrUnsupportedType{What: what}
}

// "X timed out while Y"
type ErrTimeout struct {
	Who  string
	What string
}

func (e ErrTimeout) Error() string {
	return fmt.Sprintf("%s %s %s", e.Who, errTimeoutMsg, e.What)
}

// who "timed out while" what
func newErrTimeout(who, what string) error {
	return ErrTimeout{Who: who, What: what}
}

// "X while resolving bucket path"
type ErrBucketPathResolution struct {
	What string
}

func (e ErrBucketPathResolution) Error() string {
	return fmt.Sprintf("%s %s", e.What, errBucketPathResolutionMsg)
}

// what "while resolving bucket path"
func newErrBucketPathResolution(what string) error {
	return ErrBucketPathResolution{What: what}
}

// "could not resolve X of value Y"
type ErrRecordResolution struct {
	What  string
	Value interface{}
}

func (e ErrRecordResolution) Error() string {
	return fmt.Sprintf("%s %v", errRecordResolutionMsg, e.What)
}

// "could not resolve" what "of value" value
func newErrRecordResolution(what string, value interface{}) error {
	return ErrRecordResolution{What: what}
}
