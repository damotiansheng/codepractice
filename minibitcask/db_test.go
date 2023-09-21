package minibitcask

import (
	"minibitcask/activefile"
	"reflect"
	"sync"
	"testing"
)

func TestDB_Close(t *testing.T) {
	type fields struct {
		data       map[string]*Hint
		opt        *Options
		activeFile *activefile.ActiveFile
		rwLock     *sync.RWMutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				data:       tt.fields.data,
				opt:        tt.fields.opt,
				activeFile: tt.fields.activeFile,
				rwLock:     tt.fields.rwLock,
			}
			if err := db.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Delete(t *testing.T) {
	type fields struct {
		data       map[string]*Hint
		opt        *Options
		activeFile *activefile.ActiveFile
		rwLock     *sync.RWMutex
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				data:       tt.fields.data,
				opt:        tt.fields.opt,
				activeFile: tt.fields.activeFile,
				rwLock:     tt.fields.rwLock,
			}
			if err := db.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Get(t *testing.T) {
	type fields struct {
		data       map[string]*Hint
		opt        *Options
		activeFile *activefile.ActiveFile
		rwLock     *sync.RWMutex
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				data:       tt.fields.data,
				opt:        tt.fields.opt,
				activeFile: tt.fields.activeFile,
				rwLock:     tt.fields.rwLock,
			}
			got, err := db.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Put(t *testing.T) {
	type fields struct {
		data       map[string]*Hint
		opt        *Options
		activeFile *activefile.ActiveFile
		rwLock     *sync.RWMutex
	}
	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				data:       tt.fields.data,
				opt:        tt.fields.opt,
				activeFile: tt.fields.activeFile,
				rwLock:     tt.fields.rwLock,
			}
			if err := db.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOpen(t *testing.T) {
	type args struct {
		opt *Options
		ops []Option
	}
	tests := []struct {
		name    string
		args    args
		want    *DB
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Open(tt.args.opt, tt.args.ops...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Open() got = %v, want %v", got, tt.want)
			}
		})
	}
}
