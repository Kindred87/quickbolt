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

// CaptureBytes appends values from the given channel to the given slice.
// The function executes until the channel is closed.
//
// The following slice types are supported: *[]string, *[][]byte, *[]int, *[]float32, *[]float64
//
// The mutex, if not nil, will be used during writes to the slice.
//
// timeoutLog, if not nil, is written to if a channel operation timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead. See quickbolt/common.go
func CaptureBytes(intoSlice interface{}, buffer chan []byte, mut *sync.Mutex, ctx context.Context, timeoutLog io.Writer, timeout ...time.Duration) error {
	if buffer == nil {
		return fmt.Errorf("input channel is empty")
	} else if intoSlice == nil {
		return fmt.Errorf("capture slice is nil")
	}

	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	for {
		timer := time.NewTimer(timeout[0])

		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
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
				return newErrUnsupportedType("slice")
			}

			if mut != nil {
				mut.Unlock()
			}
		case <-timer.C:
			err := newErrTimeout("byte capture", "waiting to receive from input channel")
			if timeoutLog != nil {
				logMutex.Lock()
				timeoutLog.Write([]byte(err.Error() + "\n"))
				logMutex.Unlock()
			}
			return err
		}
	}
}

// Capture appends values from the given channel to the given slice.
// The function executes until the channel is closed.
//
// The mutex, if not nil, will be used during writes to the slice.
//
// timeoutLog, if not nil, is written to if a channel operation timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead. See quickbolt/common.go
func Capture[T any](into *[]T, buffer chan T, mut *sync.Mutex, ctx context.Context, timeoutLog io.Writer, timeout ...time.Duration) error {
	if buffer == nil {
		return fmt.Errorf("input channel is empty")
	}

	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	for {
		timer := time.NewTimer(timeout[0])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case v, ok := <-buffer:
			timer.Stop()

			if !ok {
				return nil
			}

			if mut != nil {
				mut.Lock()
			}

			(*into) = append((*into), v)

			if mut != nil {
				mut.Unlock()
			}
		case <-timer.C:
			err := newErrTimeout("channel value capture", "waiting to receive from input channel")
			if timeoutLog != nil {
				logMutex.Lock()
				timeoutLog.Write([]byte(err.Error() + "\n"))
				logMutex.Unlock()
			}
			return err
		}
	}
}

// Filter passes allowed values between two channels until the input channel is closed.
//
// timeoutLog, if not nil, is written to if a channel operation timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead.
// See quickbolt/common.go
func Filter[T any](in chan T, out chan T, allow func(T) bool, ctx context.Context, timeoutLog io.Writer, timeout ...time.Duration) error {
	defer close(out)

	if in == nil {
		return fmt.Errorf("input channel is nil")
	} else if out == nil {
		return fmt.Errorf("output channel is nil")
	} else if allow == nil {
		return fmt.Errorf("allow function is nil")
	}

	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	for {
		timer := time.NewTimer(timeout[0])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
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
					err := newErrTimeout("channel filtration", "waiting to send to output channel")
					if timeoutLog != nil {
						logMutex.Lock()
						timeoutLog.Write([]byte(err.Error() + "\n"))
						logMutex.Unlock()
					}
					return err
				}
			}
		case <-timer.C:
			err := newErrTimeout("channel filtration", "waiting to receive from input channel")
			if timeoutLog != nil {
				logMutex.Lock()
				timeoutLog.Write([]byte(err.Error() + "\n"))
				logMutex.Unlock()
			}
			return err
		}

	}
}

// Convert converts values received from a channel of type A and sends them to a channel of type B.
//
// timeoutLog, if not nil, is written to if a channel operation timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead.
// See quickbolt/common.go
func Convert[A any, B any](in chan A, convert func(A) (B, error), out chan B, ctx context.Context, timeoutLog io.Writer, timeout ...time.Duration) error {
	defer close(out)

	if in == nil {
		return fmt.Errorf("input channel is nil")
	} else if out == nil {
		return fmt.Errorf("output channel is nil")
	} else if convert == nil {
		return fmt.Errorf("conversion function is nil")
	}

	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	for {
		timer := time.NewTimer(timeout[0])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case v, ok := <-in:
			timer.Stop()

			if !ok {
				return nil
			}

			new, err := convert(v)
			if err != nil {
				return fmt.Errorf("error while converting value %v: %w", v, err)
			}

			err = Send(out, new, ctx, timeoutLog, timeout...)
			if err != nil {
				return fmt.Errorf("error while sending %v to output channel: %w", new, err)
			}
		case <-timer.C:
			err := newErrTimeout("channel conversion", "waiting to receive from input channel")
			if timeoutLog != nil {
				logMutex.Lock()
				timeoutLog.Write([]byte(err.Error() + "\n"))
				logMutex.Unlock()
			}
			return err
		}
	}
}

// DoEach executes the provided function on each value received from the input channel.
//
// Do is provided the values received from the input channel, output channel, and database.
//
// WorkLimit sets the limit of goroutines if >= 1.
//
// timeoutLog, if not nil, is written to if a buffer or concurrent operation timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead.
// See quickbolt/common.go
func DoEach[T any](in chan T, db DB, do func(T, chan T, DB) error, out chan T, workLimit int, ctx context.Context, timeoutLog io.Writer, timeout ...time.Duration) error {
	defer close(out)

	var eg errgroup.Group
	if workLimit >= 1 {
		eg.SetLimit(workLimit)
	}

	if in == nil {
		return fmt.Errorf("input channel is nil")
	} else if do == nil {
		return fmt.Errorf("do func is nil")
	} else if out == nil {
		return fmt.Errorf("output channel is nil")
	}

	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	for {
		timer := time.NewTimer(timeout[0])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case v, ok := <-in:
			timer.Stop()
			if !ok {
				return eg.Wait()
			}

		goroutineSpawn:
			for {
				timer := time.NewTimer(timeout[0])
				select {
				case <-timer.C:
					err := newErrTimeout("do each execution", fmt.Sprintf("waiting to create new goroutine using %v", v))
					if timeoutLog != nil {
						logMutex.Lock()
						timeoutLog.Write([]byte(err.Error() + "\n"))
						logMutex.Unlock()
					}
					return err
				default:
					if eg.TryGo(func() error { return do(v, out, db) }) {
						break goroutineSpawn
					}
				}
			}

		case <-timer.C:
			err := newErrTimeout("do each execution", "waiting to receive from input channel")
			if timeoutLog != nil {
				logMutex.Lock()
				timeoutLog.Write([]byte(err.Error() + "\n"))
				logMutex.Unlock()
			}
			return err
		}

	}
}

// Send sends the given value to the given channel.
//
// timeoutLog, if not nil, is written to if a channel or concurrent operation timeout occurs.
//
// If a timeout is not given, quickbolt's default timeout will be used instead.
// See quickbolt/common.go
func Send[T any](buffer chan T, value T, ctx context.Context, timeoutLog io.Writer, timeout ...time.Duration) error {
	if buffer == nil {
		return fmt.Errorf("channel is nil")
	}

	if timeout == nil {
		timeout = []time.Duration{defaultBufferTimeout}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	timer := time.NewTimer(timeout[0])
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case buffer <- value:
		timer.Stop()
		return nil
	case <-timer.C:
		err := newErrTimeout(fmt.Sprintf("channel send for value %v", value), "waiting to send to channel")
		if timeoutLog != nil {
			logMutex.Lock()
			timeoutLog.Write([]byte(err.Error() + "\n"))
			logMutex.Unlock()
		}
		return err
	}
}
