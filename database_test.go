package quickbolt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testFileName = "database_test.go"
)

func Test_dbWrapper_ValuesAtErrors(t *testing.T) {
	db, err := Create("foo.db")
	assert.Nil(t, err)

	defer db.RemoveFile()

	err = db.Insert("valid", "valid", []string{"valid"})
	assert.Nil(t, err)

	type args struct {
		path      interface{}
		mustExist bool
		buffer    chan []byte
	}
	tests := []struct {
		name         string
		d            DB
		args         args
		wantContains string
	}{
		{name: "no bucket", d: db, args: args{path: []string{"test"}, mustExist: true, buffer: make(chan []byte)}, wantContains: testFileName},
		{name: "empty path", d: &dbWrapper{}, wantContains: testFileName},
		{name: "no db", d: &dbWrapper{}, args: args{path: []string{"test"}, mustExist: true, buffer: make(chan []byte)}, wantContains: testFileName},
		{name: "empty channel", d: db, args: args{path: []string{"valid"}, mustExist: true, buffer: make(chan []byte)}, wantContains: testFileName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.d.ValuesAt(tt.args.path, tt.args.mustExist, tt.args.buffer)
			if err == nil {
				t.Error("returned error was nil")
			} else if !strings.Contains(err.Error(), tt.wantContains) {
				t.Errorf("error \"%s\" did not contain %s", err.Error(), tt.wantContains)
			}
		})
	}
}
