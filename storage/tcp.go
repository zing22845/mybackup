package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mybackup/model"
	"mybackup/utils"
	"net"
	"time"

	"github.com/hako/durafmt"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/bcrypt"
)

// FS backup source or recovery destination
type TCP struct {
	model.TCP
	Type string   `json:"type,omitempty"`
	Conn net.Conn `json:"-"`
	File
}

// user defined unmarshall method
func (t *TCP) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}

	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &t.Type)
			if err != nil {
				return err
			}
		case "ip":
			err = json.Unmarshal(v, &t.IP)
			if err != nil {
				return err
			}
		case "port":
			err = json.Unmarshal(v, &t.Port)
			if err != nil {
				return err
			}
		case "password":
			err = json.Unmarshal(v, &t.Password)
			if err != nil {
				return err
			}
		case "conn_timeout":
			tmpStr := ""
			err = json.Unmarshal(v, &tmpStr)
			if err != nil {
				return err
			}
			t.ConnTimeout, err = time.ParseDuration(tmpStr)
			if err != nil {
				return err
			}
		case "send_size":
			err = json.Unmarshal(v, &t.SendSize)
			if err != nil {
				return err
			}
		case "receive_size":
			err = json.Unmarshal(v, &t.ReceiveSize)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	if t.ConnTimeout <= 30*time.Second {
		t.ConnTimeout = 30 * time.Second
	}
	return err
}

func (t *TCP) GetType() string {
	return t.Type
}

func (t *TCP) GetFilePath() string {
	if t.FilePath == "" {
		t.FilePath = fmt.Sprintf("%s_%s_%d", t.Type, t.IP, t.Port)
	}
	return t.FilePath
}

func (t *TCP) SetFilePath(filepath string) {
	t.FilePath = filepath
}

func (t *TCP) GetPrefix() string {
	return t.Prefix
}

func (t *TCP) SetPrefix(prefix string) {
	t.Prefix = prefix
}

// ReadFrom read from reader and write to tcp
func (t *TCP) ReadFrom(r io.Reader) (n int64, err error) {
	defer t.Conn.Close()
	t.SendSize, err = io.Copy(t.Conn, r)
	return t.SendSize, err
}

// WriteTo read from tcp and write to writer
func (t *TCP) WriteTo(w io.Writer) (n int64, err error) {
	defer t.Conn.Close()
	t.ReceiveSize, err = io.Copy(w, t.Conn)
	return t.ReceiveSize, err
}

// List expired files
func (t *TCP) ListExpiredByDatetime(expireDatetime *utils.SQLNullTime) (files []File, err error) {
	return nil, fmt.Errorf("unsupport action")
}

func (t *TCP) ListExpiredByDuration(expireDuration time.Duration) (files []File, err error) {
	// set base time
	baseTime := time.Now()

	// get expire time
	expireDatetime := utils.NewTime(baseTime.Add(-expireDuration))

	return t.ListExpiredByDatetime(expireDatetime)
}

// Delete expired files
func (t *TCP) DeleteExpiredByDatetime(expireDatetime *utils.SQLNullTime) (files []File, err error) {
	return nil, fmt.Errorf("unsupport action")
}

// delete expired files by duration
func (t *TCP) DeleteExpiredByDuration(expireDuration time.Duration) (files []File, err error) {
	// set base time
	baseTime := time.Now()

	// get expire time
	expireDatetime := utils.NewTime(baseTime.Add(-expireDuration))

	return t.DeleteExpiredByDatetime(expireDatetime)
}

// get file info
func (t *TCP) GetFileInfo() (fileInfo *File, err error) {
	t.FileStatus = "non-existent"
	return &t.File, nil
}

// delete file
func (t *TCP) DeleteFile() (err error) {
	return fmt.Errorf("unsupport action")
}

func (t *TCP) InitClient() (err error) {
	// dial connection
	addr := fmt.Sprintf("%s:%d", t.IP, t.Port)
	t.Conn, err = net.DialTimeout("tcp", addr, t.ConnTimeout)
	if err != nil {
		return err
	}
	if t.Password != "" {
		// auth
		magic := []byte("9d7f156c-055d-49d6-b9b4-435e2dee7512")
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(t.Password), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("process password failed")
		}
		_, err = t.Conn.Write(hashedPassword)
		if err != nil {
			fmt.Printf("send auth to %s failed: %s\n", t.Conn.RemoteAddr(), err)
		}
		buf := make([]byte, 1024)
		_, err = t.Conn.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("read auth response failed: %w", err)
		}
		err = bcrypt.CompareHashAndPassword(buf, append(magic, []byte(t.Password)...))
		if err != nil {
			return fmt.Errorf("auth failed, server not match this client")
		}
	}
	remoteAddr := t.Conn.RemoteAddr()
	t.IP = remoteAddr.(*net.TCPAddr).IP.String()
	t.Port = uint16(remoteAddr.(*net.TCPAddr).Port)
	return nil
}

func (t *TCP) InitServerListener(ctx context.Context) (listen *net.TCPListener, err error) {
	// listening on address
	listen, err = net.ListenTCP("tcp", &net.TCPAddr{Port: int(t.Port), IP: net.IP(t.IP)})
	if err != nil {
		return nil, err
	}
	// set deadline
	err = listen.SetDeadline(time.Now().Add(t.ConnTimeout))
	if err != nil {
		return nil, err
	}
	log.Infof("listening on %s, with timeout: %s", listen.Addr(), durafmt.Parse(t.ConnTimeout))
	t.IP = listen.Addr().(*net.TCPAddr).IP.String()
	t.Port = uint16(listen.Addr().(*net.TCPAddr).Port)
	// close server by context
	go func() {
		defer listen.Close()
		<-ctx.Done()
	}()
	return listen, nil
}

func (t *TCP) InitServerConn(ctx context.Context, listen *net.TCPListener) (err error) {
	// accept connection from client
	t.Conn, err = listen.Accept()
	if err != nil {
		return err
	}
	if t.Password != "" {
		// auth
		magic := []byte("9d7f156c-055d-49d6-b9b4-435e2dee7512")
		buf := make([]byte, 1024)
		_, err = t.Conn.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("auth failed with error: %w", err)
		}
		err = bcrypt.CompareHashAndPassword(buf, []byte(t.Password))
		if err != nil {
			return fmt.Errorf("auth failed, invalid password")
		}
		response, err := bcrypt.GenerateFromPassword(append(magic, []byte(t.Password)...), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("generate auth response failed")
		}
		_, err = t.Conn.Write(response)
		if err != nil {
			return err
		}
	}
	return nil
}
