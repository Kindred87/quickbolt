package quickbolt

import (
	"reflect"
	"testing"
)

func Test_resolveBucketPath(t *testing.T) {
	target := [][]byte{[]byte("foo1"), []byte("foo2"), []byte("foo3")}

	type args struct {
		p interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    [][]byte
		wantErr bool
	}{
		{name: "Basic", args: args{p: []string{"foo1", "foo2", "foo3"}}, want: target, wantErr: false},
		{name: "Incorrect type", args: args{p: []int{1, 2, 3}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveBucketPath(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveBucketPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resolveBucketPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
