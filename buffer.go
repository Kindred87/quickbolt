package quickbolt

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// CaptureBytes appends values from the given buffer to the given slice.
// The function executes until the buffer is closed.
//
// The following slice types are supported: *[]string, *[][]byte, *[]int, *[]float32, *[]float64
//
// The mutex, if not nil, will be used during writes to the slice.
//
// timeoutLog, if not nil, is written to if a buffer operation timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead.
// See quickbolt/common.go
func CaptureBytes(intoSlice interface{}, buffer chan []byte, mut *sync.Mutex, ctx *context.Context, timeoutLog *io.Writer, timeout ...time.Duration) error {
	if buffer == nil {
		return fmt.Errorf("input buffer is empty")
	} else if intoSlice == nil {
		return fmt.Errorf("capture slice is nil")
	}

	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		c := context.Background()
		ctx = &c
	}

	for {
		timer := time.NewTimer(timeout[0])

		select {
		case <-(*ctx).Done():
			timer.Stop()
			return (*ctx).Err()
		case v, ok := <-buffer:
			timer.Stop()

			if !ok {
				return nil
			}

			if mut != nil {
				mut.Lock()
			}

			switch sl := intoSlice.(type) {
			case *[]string:
				*sl = append(*sl, string(v))
			case *[][]byte:
				*sl = append(*sl, v)
			case *[]int:
				i, err := strconv.Atoi(string(v))
				if err != nil {
					return fmt.Errorf("error while converting %s to an integer: %w", string(v), err)
				}
				*sl = append(*sl, i)
			case *[]float32:
				f, err := strconv.ParseFloat(string(v), 32)
				if err != nil {
					return fmt.Errorf("error while parsing %s as a 32 bit float: %w", string(v), err)
				}
				*sl = append(*sl, float32(f))
			case *[]float64:
				f, err := strconv.ParseFloat(string(v), 32)
				if err != nil {
					return fmt.Errorf("error while parsing %s as a 32 bit float: %w", string(v), err)
				}
				*sl = append(*sl, f)
			default:
				return fmt.Errorf("slice type is unsupported")
			}

			if mut != nil {
				mut.Unlock()
			}
		case <-timer.C:
			err := fmt.Errorf("byte capture timed out while waiting to receive from input buffer")
			if timeoutLog != nil {
				(*timeoutLog).Write([]byte(err.Error()))
			}
			return err
		}
	}
}

// Filter passes allowed values between two buffers until the input buffer is
// closed.
//
// timeoutLog, if not nil, is written to if a buffer operation timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead.
// See quickbolt/common.go
func Filter(in chan []byte, out chan []byte, allow func([]byte) bool, ctx *context.Context, timeoutLog *io.Writer, timeout ...time.Duration) error {
	defer close(out)

	if in == nil {
		return fmt.Errorf("input buffer is empty")
	} else if out == nil {
		return fmt.Errorf("output buffer is empty")
	} else if allow == nil {
		return fmt.Errorf("allow function is nil")
	}

	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		c := context.Background()
		ctx = &c
	}

	for {
		timer := time.NewTimer(timeout[0])
		select {
		case <-(*ctx).Done():
			timer.Stop()
			return (*ctx).Err()
		case v, ok := <-in:
			timer.Stop()

			if !ok {
				return nil
			}

			if allow(v) {
				timer := time.NewTimer(timeout[0])
				select {
				case out <- v:
					timer.Stop()
				case <-timer.C:
					err := fmt.Errorf("buffer filtration timed out while waiting to send to output buffer")
					if timeoutLog != nil {
						(*timeoutLog).Write([]byte(err.Error()))
					}
					return err
				}
			}
		case <-timer.C:
			err := fmt.Errorf("buffer filtration timed out while waiting to receive from input buffer")
			if timeoutLog != nil {
				(*timeoutLog).Write([]byte(err.Error()))
			}
			return err
		}

	}
}

// DoEach executes the provided function on each value received from the input
// buffer.
//
// Do is provided the out buffer and values received from the input buffer.
//
// Do is executed concurrently if eg is not nil.
//
// timeoutLog, if not nil, is written to if a buffer or concurrent operation
// timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead.
// See quickbolt/common.go
func DoEach(in chan []byte, do func([]byte, chan []byte) error, out chan []byte, eg *errgroup.Group, ctx *context.Context, timeoutLog *io.Writer, timeout ...time.Duration) error {
	defer close(out)

	if in == nil {
		return fmt.Errorf("input buffer is nil")
	} else if do == nil {
		return fmt.Errorf("do func is nil")
	} else if out == nil {
		return fmt.Errorf("output buffer is nil")
	}
	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		c := context.Background()
		ctx = &c
	}

	for {
		timer := time.NewTimer(timeout[0])
		select {
		case <-(*ctx).Done():
			timer.Stop()
			return (*ctx).Err()
		case v, ok := <-in:
			timer.Stop()
			if !ok {
				return nil
			}

			if eg == nil {
				err := do(v, out)
				if err != nil {
					return fmt.Errorf("error while executing do func on value %s", string(v))
				}
				continue
			}

		goroutineSpawn:
			for {
				timer := time.NewTimer(timeout[0])
				select {
				case <-timer.C:
					err := fmt.Errorf("do each execution timed out while waiting to create new goroutine using %s", string(v))
					if timeoutLog != nil {
						(*timeoutLog).Write([]byte(err.Error()))
					}
					return err
				default:
					if eg.TryGo(func() error { return do(v, out) }) {
						break goroutineSpawn
					}
				}
			}

		case <-timer.C:
			err := fmt.Errorf("do each execution timed out while waiting to receive from input buffer")
			if timeoutLog != nil {
				(*timeoutLog).Write([]byte(err.Error()))
			}
			return err
		}

	}
}