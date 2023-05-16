package storage

import "time"

const (
	DEFAULT_LIST_LIMIT = 10000
	MAX_LIST_LIMIT     = 100000
)

type File struct {
	FilePath         string    `json:"file_path,omitempty"`
	Bucket           string    `json:"bucket,omitempty"`
	Prefix           string    `json:"prefix,omitempty"`
	ModifyTime       time.Time `json:"modify_time,omitempty"`
	Size             int64     `json:"size,omitempty"`
	UncompressedSize int64     `json:"uncompressed_size,omitempty"`
	Count            int64     `json:"count,omitempty"`
	FileType         string    `json:"file_type,omitempty"`
	StorageClass     string    `json:"storage_class,omitempty"`
	ListLimit        uint32    `json:"list_limit,omitempty"`
	FileStatus       string    `json:"-"`
}
