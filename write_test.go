package quickbolt

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func Test_insertValue(t *testing.T) {
	db, err := bbolt.Open("foo.db", 0600, nil)
	assert.Nil(t, err)
	defer os.Remove(db.Path())
	defer db.Close()

	_, err = PerEndian(uint64(1))
	assert.Nil(t, err)

	type args struct {
		db    *bbolt.DB
		value []byte
		path  [][]byte
	}
	type check struct {
		key   []byte
		value []byte
		path  [][]byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		check   check
	}{
		{name: "Basic", args: args{db: db, value: []byte("test-value"), path: [][]byte{}}, wantErr: false, check: check{key: []byte("1"), value: []byte("test-value"), path: [][]byte{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := insertValue(tt.args.db, tt.args.value, tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("insertValue() error = %v, wantErr %v", err, tt.wantErr)
			}

			b, err := getValue(tt.args.db, tt.check.key, tt.args.path, true)
			if !tt.wantErr {
				assert.Nil(t, err)
			}

			assert.Equal(t, tt.check.value, b)
		})
	}
}
