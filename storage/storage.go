package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"mybackup/helper"
)

// Storage backup or recovery source or target, can be MySQL, S3 or FS
type Storage interface {
	UnmarshalJSON(data []byte) (err error)
	GetType() string
	GetFilePath() string
	SetFilePath(filepath string)
	GetPrefix() string
	SetPrefix(prefix string)
	GetFileInfo() (fileInfo *File, err error)
	DeleteFile() error
	io.ReaderFrom
	io.WriterTo
	Cleaner
}

func UnmarshalStorage(data []byte) (s Storage, err error) {
	typeHelper := helper.TypeHelper{}
	err = json.Unmarshal(data, &typeHelper)
	if err != nil {
		return nil, fmt.Errorf("unmarshal type of target failed: %w", err)
	}
	switch typeHelper.Type {
	case "s3":
		s = &S3{}
	case "fs":
		s = &FS{}
	case "tcp":
		s = &TCP{}
	default:
		return nil, fmt.Errorf("unknown storage type: %s", typeHelper.Type)
	}
	err = json.Unmarshal(data, s)
	if err != nil {
		return nil, err
	}
	return s, nil
}
