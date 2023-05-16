package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mybackup/utils"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hako/durafmt"
	log "github.com/sirupsen/logrus"
)

// FS backup source or recovery destination
type FS struct {
	Type                string   `json:"type,omitempty"`
	ExcludeDIRs         []string `json:"exclude_dirs,omitempty"`
	ExtraFiles          []File   `json:"extra_files,omitempty"`
	TarArcname          string   `json:"-"`
	TarIgnoreFileChange bool     `json:"-"`
	TarIgnoreAllErrors  bool     `json:"-"`
	File
}

// user defined unmarshall method
func (fs *FS) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}

	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &fs.Type)
			if err != nil {
				return err
			}
		case "file_path":
			err = json.Unmarshal(v, &fs.FilePath)
			if err != nil {
				return err
			}
		case "prefix":
			err = json.Unmarshal(v, &fs.Prefix)
			if err != nil {
				return err
			}
		case "exclude_dirs":
			err = json.Unmarshal(v, &fs.ExcludeDIRs)
			if err != nil {
				return err
			}
		case "extra_files":
			err = json.Unmarshal(v, &fs.ExtraFiles)
			if err != nil {
				return err
			}
		case "list_limit":
			err = json.Unmarshal(v, &fs.ListLimit)
			if err != nil {
				return err
			}
		case "tar_arcname":
			err = json.Unmarshal(v, &fs.TarArcname)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	return err
}

func (fs *FS) GetType() string {
	return fs.Type
}

func (fs *FS) GetFilePath() string {
	return fs.FilePath
}

func (fs *FS) SetFilePath(filepath string) {
	fs.FilePath = filepath
}

func (fs *FS) GetPrefix() string {
	return fs.Prefix
}

func (fs *FS) SetPrefix(prefix string) {
	fs.Prefix = prefix
}

// ReadFrom read from reader and write to fs
func (fs *FS) ReadFrom(r io.Reader) (n int64, err error) {
	var output io.WriteCloser

	if fs.FilePath == "" {
		return 0, fmt.Errorf("no file path was specified")
	}

	if fs.FilePath == "-" {
		// upload from stdin
		log.Infof("writing stream to stdout")
		output = os.Stdout
	} else {
		if fs.FileType == "directory" {
			log.Infof("untar stream to directory(%s)",
				fs.FilePath)
			fs.Size, err = utils.Untar(r, fs.FilePath)
			return fs.Size, err
		}
		baseDIR := filepath.Dir(fs.FilePath)
		// create dir if not exists
		err := utils.CreateDIR(baseDIR)
		if err != nil {
			return 0, err
		}
		// create file
		output, err = os.Create(fs.FilePath)
		if err != nil {
			return 0, err
		}
		defer output.Close()
	}
	fs.Size, err = io.Copy(output, r)
	return fs.Size, err
}

// WriteTo read from fs and write to writer
func (fs *FS) WriteTo(w io.Writer) (n int64, err error) {
	var input io.ReadCloser
	if fs.FilePath == "" {
		return 0, fmt.Errorf("no file path was specified")
	}

	if fs.FilePath == "-" {
		// upload from stdin
		log.Infof("reading stream from stdin")
		input = os.Stdin
	} else {
		// upload directory or single file
		info, err := os.Stat(fs.FilePath)
		if err != nil {
			return 0, err
		}
		if info.IsDir() {
			log.Infof("packing directory(%s) with tar", fs.FilePath)
			fs.Size, err = utils.Tar(fs.FilePath, fs.TarArcname, fs.ExcludeDIRs, fs.TarIgnoreFileChange, fs.TarIgnoreAllErrors, w)
			return fs.Size, err
		}
		input, err = os.Open(fs.FilePath)
		if err != nil {
			return 0, fmt.Errorf("open file %s failed: %w", fs.FilePath, err)
		}
	}
	fs.Size, err = io.Copy(w, input)
	return fs.Size, err
}

// List expired files
func (fs *FS) ListExpiredByDatetime(expireDatetime *utils.SQLNullTime) (files []File, err error) {
	FileList := []File{
		{
			Prefix: fs.Prefix,
		},
	}
	FileList = append(FileList, fs.ExtraFiles...)

	// check list limit
	if fs.ListLimit == 0 {
		fs.ListLimit = DEFAULT_LIST_LIMIT
		log.Infof(
			"set default list_limit: %d",
			DEFAULT_LIST_LIMIT,
		)
	} else if fs.ListLimit > MAX_LIST_LIMIT {
		return nil, fmt.Errorf("current list_limit(%d) exceeded max value: %d",
			fs.ListLimit, MAX_LIST_LIMIT)
	}

	for _, file := range FileList {
		// get fs pattern
		pattern := fmt.Sprintf("%s*", file.Prefix)

		// get baseDIR
		baseDIR := filepath.Dir(file.Prefix)

		// check files in basedir
		var fileCount uint32
		var listLimitErr = fmt.Errorf("file count reach the list limit(%d)", fs.ListLimit)
		err = filepath.Walk(baseDIR, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				match, err := filepath.Match(pattern, path)
				if err != nil {
					return err
				}
				if match && info.ModTime().Before(expireDatetime.Time) {
					f := File{
						FilePath:   path,
						Prefix:     file.Prefix,
						ModifyTime: info.ModTime(),
						Size:       info.Size(),
						FileStatus: "non-existent",
					}
					files = append(files, f)
					fileCount++
					if fileCount >= fs.ListLimit {
						return listLimitErr
					}
				}
			}
			return nil
		})
		if errors.Is(err, listLimitErr) {
			log.Infof(listLimitErr.Error())
			err = nil
			continue
		}
		if err != nil {
			return nil, err
		}
	}

	// sort files by ModifyTime
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModifyTime.After(files[j].ModifyTime)
	})

	for _, f := range files {
		log.Infof(
			"file name: %s, last modify time: %s, size: %s",
			f.FilePath,
			f.ModifyTime.Format("2006-01-02 15:04:05"),
			humanize.Bytes(uint64(f.Size)),
		)
	}
	return files, nil
}

func (fs *FS) ListExpiredByDuration(expireDuration time.Duration) (files []File, err error) {
	// set base time
	baseTime := time.Now()
	// get expire time
	expireDatetime := utils.NewTime(baseTime.Add(-expireDuration))
	return fs.ListExpiredByDatetime(expireDatetime)
}

// Delete expired files
func (fs *FS) DeleteExpiredByDatetime(expireDatetime *utils.SQLNullTime) (files []File, err error) {
	if fs.Prefix == "" {
		return nil, fmt.Errorf("can not remove expired files without prefix")
	}
	baseTime := time.Now()

	// check if expire datetime in one day, reset expireDatetime
	earliestExpireDatetime := baseTime.Add(-MIN_EXPIRE_DURATION)
	if expireDatetime.Time.After(earliestExpireDatetime) {
		expireDatetime.SetTime(earliestExpireDatetime)
		log.Warnf(
			"given expire datetime must before %s, reset expire_datetime to %s",
			durafmt.Parse(MIN_EXPIRE_DURATION).String(), expireDatetime.Time,
		)
	}

	expiredFiles, err := fs.ListExpiredByDatetime(expireDatetime)
	if err != nil {
		return nil, err
	}

	for _, f := range expiredFiles {
		err := os.Remove(f.FilePath)
		if err != nil {
			log.Warnf("delete file %s failed", f.FilePath)
			f.FileStatus = "failed to delete"
			continue
		}
		f.FileStatus = "deleted"
		files = append(files, f)
	}
	return files, nil
}

// delete expired files by duration
func (fs *FS) DeleteExpiredByDuration(expireDuration time.Duration) (files []File, err error) {
	// set base time
	baseTime := time.Now()

	// get expire time
	expireDatetime := utils.NewTime(baseTime.Add(-expireDuration))

	return fs.DeleteExpiredByDatetime(expireDatetime)
}

// get file info
func (fs *FS) GetFileInfo() (fileInfo *File, err error) {
	info, err := os.Stat(fs.FilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fs.FileStatus = "non-existent"
			return &fs.File, nil
		} else {
			return nil, err
		}
	}
	fs.ModifyTime = info.ModTime()
	if info.IsDir() {
		fs.FileType = "directory"
		fs.Count, fs.Size, err = utils.DIRCount(fs.FilePath, fs.ExcludeDIRs)
		if err != nil {
			return nil, err
		}
	} else {
		fs.Size = info.Size()
		fs.FileType, err = utils.GetFileContentType(fs.FilePath)
		if fs.FileType == "application/x-gzip" {
			uncompressedSize, err := utils.GetGzipUncompressedSize(fs.FilePath)
			if err != nil {
				return nil, err
			}
			fs.UncompressedSize = int64(uncompressedSize)
		}
		fs.Count = 1
		if err != nil {
			return nil, err
		}
	}
	fs.FileStatus = "existent"
	return &fs.File, nil
}

// delete file
func (fs *FS) DeleteFile() (err error) {
	log.Infof("deleting file: %s", fs.FilePath)
	return os.Remove(fs.FilePath)
}
