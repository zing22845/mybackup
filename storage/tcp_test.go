package storage

import (
	"context"
	"mybackup/model"
	"net"
	"reflect"
	"testing"
)

func TestTCP_InitClient(t *testing.T) {
	type fields struct {
		TCP  model.TCP
		Type string
		Conn net.Conn
		File File
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
			tr := &TCP{
				TCP:  tt.fields.TCP,
				Type: tt.fields.Type,
				Conn: tt.fields.Conn,
				File: tt.fields.File,
			}
			if err := tr.InitClient(); (err != nil) != tt.wantErr {
				t.Errorf("TCP.InitClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTCP_GetFileInfo(t *testing.T) {
	type fields struct {
		TCP  model.TCP
		Type string
		Conn net.Conn
		File File
	}
	tests := []struct {
		name         string
		fields       fields
		wantFileInfo *File
		wantErr      bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TCP{
				TCP:  tt.fields.TCP,
				Type: tt.fields.Type,
				Conn: tt.fields.Conn,
				File: tt.fields.File,
			}
			gotFileInfo, err := tr.GetFileInfo()
			if (err != nil) != tt.wantErr {
				t.Errorf("TCP.GetFileInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotFileInfo, tt.wantFileInfo) {
				t.Errorf("TCP.GetFileInfo() = %v, want %v", gotFileInfo, tt.wantFileInfo)
			}
		})
	}
}

func TestTCP_InitServerListener(t *testing.T) {
	type fields struct {
		TCP  model.TCP
		Type string
		Conn net.Conn
		File File
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantListen *net.TCPListener
		wantErr    bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TCP{
				TCP:  tt.fields.TCP,
				Type: tt.fields.Type,
				Conn: tt.fields.Conn,
				File: tt.fields.File,
			}
			gotListen, err := tr.InitServerListener(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("TCP.InitServerListener() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotListen, tt.wantListen) {
				t.Errorf("TCP.InitServerListener() = %v, want %v", gotListen, tt.wantListen)
			}
		})
	}
}

func TestTCP_InitServerConn(t *testing.T) {
	type fields struct {
		TCP  model.TCP
		Type string
		Conn net.Conn
		File File
	}
	type args struct {
		ctx    context.Context
		listen *net.TCPListener
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
			tr := &TCP{
				TCP:  tt.fields.TCP,
				Type: tt.fields.Type,
				Conn: tt.fields.Conn,
				File: tt.fields.File,
			}
			if err := tr.InitServerConn(tt.args.ctx, tt.args.listen); (err != nil) != tt.wantErr {
				t.Errorf("TCP.InitServerConn() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
