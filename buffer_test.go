package quickbolt

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestCaptureBytes(t *testing.T) {
	var stringSlice []string

	type args struct {
		intoSlice  interface{}
		buffer     chan []byte
		mut        *sync.Mutex
		ctx        *context.Context
		timeoutLog io.Writer
		timeout    []time.Duration
	}
	tests := []struct {
		name    string
		args    args
		send    [][]byte
		wantErr bool
		success func() bool
	}{
		{name: "String", args: args{intoSlice: &stringSlice, buffer: make(chan []byte), timeoutLog: os.Stdout}, send: [][]byte{[]byte("foo1")}, wantErr: false, success: func() bool { return stringSlice[0] == "foo1" }},
		{name: "No slice", args: args{buffer: make(chan []byte)}, wantErr: true},
		{name: "Uint slice", args: args{intoSlice: &[]uint{}, buffer: make(chan []byte)}, send: [][]byte{[]byte(strconv.FormatUint(1, 2))}, wantErr: true},
		{name: "Mutex", args: args{intoSlice: &stringSlice, buffer: make(chan []byte), mut: &sync.Mutex{}}, send: [][]byte{[]byte("foo2")}, wantErr: false},
		{name: "No buffer", args: args{intoSlice: &stringSlice}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var eg errgroup.Group

			buf := tt.args.buffer
			send := tt.send
			name := tt.name
			eg.Go(func() error {
				if buf == nil {
					return nil
				}
				defer close(buf)
				for _, s := range send {
					timer := time.NewTimer(defaultBufferTimeout)
					select {
					case buf <- s:
						timer.Stop()
					case <-timer.C:
						return fmt.Errorf("goroutine sending values to buffer timed out while sending %s during test %s", string(s), name)
					}
				}
				return nil
			})

			if err := CaptureBytes(tt.args.intoSlice, tt.args.buffer, tt.args.mut, tt.args.ctx, tt.args.timeoutLog, tt.args.timeout...); (err != nil) != tt.wantErr {
				t.Errorf("CaptureBytes() error = %v, wantErr %v", err, tt.wantErr)
			}

			assert.Nil(t, eg.Wait())

			if tt.success != nil {
				assert.True(t, tt.success())
			}
		})
	}
}

func TestFilter(t *testing.T) {
	type args struct {
		in      chan []byte
		out     chan []byte
		allow   func([]byte) bool
		ctx     *context.Context
		logger  io.Writer
		timeout []time.Duration
	}
	tests := []struct {
		name     string
		args     args
		send     []byte
		wantSent bool
		wantErr  bool
	}{
		{name: "Basic", args: args{in: make(chan []byte), out: make(chan []byte), allow: func(b []byte) bool { return bytes.Equal(b, []byte("foo")) }, logger: os.Stdout}, send: []byte("foo"), wantSent: true, wantErr: false},
		{name: "Filter out", args: args{in: make(chan []byte), out: make(chan []byte), allow: func(b []byte) bool { return bytes.Equal(b, []byte("foo")) }}, send: []byte("dark foo"), wantSent: false, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var eg errgroup.Group

			in := tt.args.in
			send := tt.send
			name := tt.name
			eg.Go(func() error {
				defer close(in)

				timer := time.NewTimer(time.Millisecond * 10)
				select {
				case in <- send:
					timer.Stop()
					return nil
				case <-timer.C:
					return fmt.Errorf("goroutine sending %s to input buffer to test %s timed out", string(send), name)
				}
			})

			out := tt.args.out
			want := tt.wantSent
			eg.Go(func() error {
				for {
					timer := time.NewTimer(time.Millisecond * 10)
					select {
					case v := <-out:
						timer.Stop()
						if want {
							assert.Equal(t, v, send)
						}
						return nil
					case <-timer.C:
						if want {
							return fmt.Errorf("goroutine receiving from out buffer for test %s timed out", name)
						}
						return nil
					}
				}

			})

			if err := Filter(tt.args.in, tt.args.out, tt.args.allow, tt.args.ctx, tt.args.logger, tt.args.timeout...); (err != nil) != tt.wantErr {
				t.Errorf("Filter() error = %v, wantErr %v", err, tt.wantErr)
			}

			assert.Nil(t, eg.Wait())
		})
	}
}
