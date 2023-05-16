package object

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"mybackup/model"
	"mybackup/storage"
	"mybackup/utils"

	"github.com/hako/durafmt"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// backup source or target file
type File struct {
	model.FileBackup
	Type                string          `json:"type,omitempty"`
	StorageID           string          `json:"storage_id,omitempty"`
	ExpireFiles         []storage.File  `json:"expire_files,omitempty"`
	ListAllFiles        []storage.File  `json:"list_all_files,omitempty"`
	TarIgnoreFileChange bool            `json:"tar_ignore_file_change,omitempty"`
	TarIgnoreAllErrors  bool            `json:"tar_ignore_all_errors,omitempty"`
	TarArcname          string          `json:"tar_arcname,omitempty"`
	Storage             storage.Storage `json:"storage,omitempty"`
	Stream              *Stream         `json:"-"`
	DefaultBaseDIR      string          `json:"-"`
	DefaultFilename     string          `json:"-"`
	MetaDB              *gorm.DB        `json:"-"`
}

// user defined unmarshall method
func (f *File) UnmarshalJSON(data []byte) (err error) {
	f.TarIgnoreFileChange = true
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}

	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &f.Type)
			if err != nil {
				return err
			}
		case "storage_id":
			err = json.Unmarshal(v, &f.StorageID)
			if err != nil {
				return err
			}
		case "storage":
			f.Storage, err = storage.UnmarshalStorage(v)
			if err != nil {
				return err
			}
		case "expire_method":
			err = json.Unmarshal(v, &f.ExpireMethod)
			if err != nil {
				return err
			}
			if !utils.Contains(storage.SupportExpireMethod, f.ExpireMethod) {
				return fmt.Errorf("unsupport expired method: %s", f.ExpireMethod)
			}
		case "expire_datetime":
			t := time.Time{}
			err = json.Unmarshal(v, &t)
			if err != nil {
				return err
			}
			f.ExpireDatetime = utils.NewTime(t)
		case "expire_duration":
			tmpStr := ""
			err = json.Unmarshal(v, &tmpStr)
			if err != nil {
				return err
			}
			f.ExpireDuration, err = time.ParseDuration(tmpStr)
			if err != nil {
				return err
			}
		case "origin_size":
			err = json.Unmarshal(v, &f.OriginSize)
			if err != nil {
				return err
			}
		case "size":
			err = json.Unmarshal(v, &f.Size)
			if err != nil {
				return err
			}
		case "pack_method":
			err = json.Unmarshal(v, &f.PackMethod)
			if err != nil {
				return err
			}
		case "unpack_method":
			err = json.Unmarshal(v, &f.UnpackMethod)
			if err != nil {
				return err
			}
		case "compress_method":
			err = json.Unmarshal(v, &f.CompressMethod)
			if err != nil {
				return err
			}
		case "decompress_method":
			err = json.Unmarshal(v, &f.DecompressMethod)
			if err != nil {
				return err
			}
		case "encrypt_method":
			err = json.Unmarshal(v, &f.EncryptMethod)
			if err != nil {
				return err
			}
		case "encrypt_key":
			err = json.Unmarshal(v, &f.EncryptKey)
			if err != nil {
				return err
			}
		case "decrypt_method":
			err = json.Unmarshal(v, &f.DecryptMethod)
			if err != nil {
				return err
			}
		case "decrypt_key":
			err = json.Unmarshal(v, &f.DecryptKey)
			if err != nil {
				return err
			}
		case "md5_sum":
			err = json.Unmarshal(v, &f.MD5Sum)
			if err != nil {
				return err
			}
		case "base_dir":
			err = json.Unmarshal(v, &f.BaseDIR)
			if err != nil {
				return err
			}
		case "speed_limit":
			err = json.Unmarshal(v, &f.SpeedLimit)
			if err != nil {
				return err
			}
		case "tar_ignore_file_change":
			err = json.Unmarshal(v, &f.TarIgnoreFileChange)
			if err != nil {
				return err
			}
		case "tar_ignore_all_errors":
			err = json.Unmarshal(v, &f.TarIgnoreAllErrors)
			if err != nil {
				return err
			}
		case "tar_arcname":
			err = json.Unmarshal(v, &f.TarArcname)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	if f.ExpireMethod == "by_datetime" && f.ExpireDatetime == nil {
		return fmt.Errorf("no expire_datetime specified")
	} else if f.ExpireMethod == "by_duration" && f.ExpireDuration <= 0 {
		return fmt.Errorf(
			"invalid expire duration: %s",
			durafmt.Parse(f.ExpireDuration).String(),
		)
	}
	f.Timers = make(map[string]*utils.Timer)
	return nil
}

func (f *File) GetType() string {
	return f.Type
}

// Move
func (f *File) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFile, ok := (targetObject).(*File)
	if !ok {
		return fmt.Errorf("only support File as target now")
	}
	// calculate time
	moveTimer := utils.NewTimer()
	targetFile.Timers["move"] = moveTimer
	defer func() {
		moveTimer.Stop()
		log.Infof("duration of backup: %s",
			moveTimer.Duration.String(),
		)
	}()

	err = f.Upload(ctx, u, targetObject, overwrite)
	if err != nil {
		return err
	}

	err = f.Storage.DeleteFile()
	if err != nil {
		return err
	}

	return err
}

// Upload
// nolint:gocyclo
func (f *File) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFile, ok := (targetObject).(*File)
	if !ok {
		return fmt.Errorf("only support File as target now")
	}
	// calculate time
	uploadTimer := utils.NewTimer()
	targetFile.Timers["upload"] = uploadTimer
	defer func() {
		uploadTimer.Stop()
		log.Infof("duration of uploading: %s",
			uploadTimer.Duration.String(),
		)
	}()
	// get source file info and set targetFile's original file info
	sourceFilePath := f.Storage.GetFilePath()
	if sourceFilePath == "" {
		return fmt.Errorf("unsupport empty source FilePath on %s", f.Storage.GetType())
	}
	if sourceFilePath != "-" {
		sourceFileInfo, err := f.Storage.GetFileInfo()
		if err != nil {
			return err
		}
		targetFile.OriginFileType = sourceFileInfo.FileType
		targetFile.OriginSize = sourceFileInfo.Size
		targetFile.OriginCount = sourceFileInfo.Count
		log.Infof("from %s filepath: %s", f.Storage.GetType(), sourceFilePath)
	} else {
		targetFile.OriginFileType = "stdin"
		targetFile.OriginSize = f.Size
		if targetFile.OriginSize <= 0 {
			targetFile.OriginSize = -1
		}
		targetFile.OriginCount = f.Count
		if targetFile.OriginCount <= 0 {
			targetFile.OriginCount = -1
		}
		log.Infof("from stdin")
	}
	log.Infof("source file type: %s", targetFile.OriginFileType)
	log.Infof("source file size: %d", targetFile.OriginSize)
	log.Infof("source file count: %d", targetFile.OriginCount)
	// set file extention
	targetFileSuffix := ""
	if targetFile.PackMethod == "tar" && targetFile.OriginFileType == "directory" {
		targetFileSuffix = ".tar"
	}
	if targetFile.CompressMethod == "gzip" {
		targetFileSuffix += ".gz"
	}
	if targetFile.EncryptMethod != "" {
		targetFileSuffix += ".enc"
	}
	// storing target file from stream
	sourceFileName := filepath.Base(sourceFilePath)
	targetFile.Stream = NewStream(ctx, 5*time.Second, sourceFileName)
	targetFile.Stream.ExpectInputChecksum = f.MD5Sum
	targetFile.Stream.InputSize = f.Size
	targetFile.Stream.OutputSize = f.Size
	targetFile.Stream.InputSpeedLimit = f.SpeedLimit
	targetFile.Stream.OutputSpeedLimit = targetFile.SpeedLimit
	targetFile.Stream.CompressMethod = targetFile.CompressMethod
	targetFile.Stream.DecompressMethod = targetFile.DecompressMethod
	targetFile.Stream.UnpackMethod = targetFile.UnpackMethod
	targetFile.Stream.EncryptMethod = targetFile.EncryptMethod
	targetFile.Stream.EncryptKey = targetFile.EncryptKey
	targetFile.Stream.DecryptMethod = targetFile.DecryptMethod
	targetFile.Stream.DecryptKey = targetFile.DecryptKey
	err = targetFile.Stream.Store(u, targetFile.Storage, targetFile.DefaultFilename, overwrite)
	if err != nil {
		return err
	}
	// set tar options
	if fs, ok := f.Storage.(*storage.FS); ok {
		fs.TarIgnoreFileChange = targetFile.TarIgnoreFileChange
		log.Infof("fs.TarIgnoreFileChange: %v", fs.TarIgnoreFileChange)
		fs.TarIgnoreAllErrors = targetFile.TarIgnoreAllErrors
		log.Infof("fs.TarIgnoreAllErrors: %v", fs.TarIgnoreFileChange)
		fs.TarArcname = targetFile.TarArcname
	}
	targetFile.Size, err = (f.Storage).WriteTo(targetFile.Stream.InputWriter)
	if err != nil {
		targetFile.Err = err
		return err
	}
	log.Infof("source write to stream succeeded")
	// cancel and wait stream done
	targetFile.Stream.Cancel()
	targetFile.Stream.WaitGroup.Wait()
	if targetFile.Stream.Err != nil {
		return targetFile.Stream.Err
	}
	// get md5sum and compare with origin
	targetFile.MD5Sum = targetFile.Stream.OutputChecksum
	return err
}

// Download
func (f *File) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFile, ok := (targetObject).(*File)
	if !ok {
		return fmt.Errorf("only support file as download target now")
	}
	_, ok = (targetFile.Storage).(*storage.FS)
	if !ok {
		return fmt.Errorf("only support local file system as download target now")
	}

	// calculate time
	downloadTimer := utils.NewTimer()
	targetFile.Timers["download"] = downloadTimer
	defer func() {
		downloadTimer.Stop()
		log.Infof("duration of downloading: %s",
			downloadTimer.Duration.String(),
		)
	}()

	// check source file
	sourceFilePath := f.Storage.GetFilePath()
	if sourceFilePath == "" {
		return fmt.Errorf("unsupport empty source FilePath on %s", f.Storage.GetType())
	}
	// check source file existence
	sourceFileInfo, err := f.Storage.GetFileInfo()
	if err != nil {
		return fmt.Errorf("get file(%s) stat failed: %w", sourceFilePath, err)
	}
	if sourceFileInfo.FileStatus == "non-existent" {
		return fmt.Errorf("source file(%s) does not exist", sourceFilePath)
	}

	// get source file info
	f.FileType = sourceFileInfo.FileType
	f.Size = sourceFileInfo.Size
	log.Infof("from %s filepath: %s", f.Storage.GetType(), sourceFilePath)
	log.Infof("source file type: %s", f.FileType)
	log.Infof("source file size: %d", f.Size)

	// storing target file from stream
	sourceFileName := filepath.Base(sourceFilePath)
	targetFile.Stream = NewStream(ctx, 5*time.Second, sourceFileName)
	targetFile.Stream.ExpectInputChecksum = f.MD5Sum
	targetFile.Stream.InputSize = f.Size
	targetFile.Stream.OutputSize = f.OriginSize
	targetFile.Stream.InputSpeedLimit = f.SpeedLimit
	targetFile.Stream.OutputSpeedLimit = targetFile.SpeedLimit
	targetFile.Stream.CompressMethod = targetFile.CompressMethod
	targetFile.Stream.DecompressMethod = targetFile.DecompressMethod
	targetFile.Stream.UnpackMethod = targetFile.UnpackMethod
	targetFile.Stream.EncryptMethod = targetFile.EncryptMethod
	targetFile.Stream.EncryptKey = targetFile.EncryptKey
	targetFile.Stream.DecryptMethod = targetFile.DecryptMethod
	targetFile.Stream.DecryptKey = targetFile.DecryptKey
	err = targetFile.Stream.Store(u, targetFile.Storage, targetFile.DefaultFilename, overwrite)
	if err != nil {
		return err
	}

	// read from source storage and write to target stream
	targetFile.Size, err = (f.Storage).WriteTo(targetFile.Stream.InputWriter)
	if err != nil {
		targetFile.Err = err
		return err
	}
	log.Infof("source write to stream succeeded")

	// cancel and wait stream done
	targetFile.Stream.Cancel()
	targetFile.Stream.WaitGroup.Wait()
	if targetFile.Stream.Err != nil {
		return targetFile.Stream.Err
	}
	// get md5sum and compare with origin
	targetFile.MD5Sum = targetFile.Stream.OutputChecksum
	return err
}

// Backup
func (f *File) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFile, ok := (targetObject).(*File)
	if !ok {
		return fmt.Errorf("only support File as target now")
	}

	err = f.Upload(ctx, u, targetObject, overwrite)
	if err != nil {
		return err
	}

	var warn error
	if targetFile.ExpireMethod != "" {
		// deleting expired files
		warn = targetFile.DeleteFiles()
		if warn != nil {
			log.Warnf("delete expired file failed: %s", warn.Error())
		}
	}

	return err
}

// Recover
func (f *File) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	// download
	err = f.Download(ctx, u, targetObject, overwrite)
	if err != nil {
		return err
	}
	return err
}

func (f *File) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("not support yet")
}

func (f *File) GetStorageFilePath() string {
	return f.Storage.GetFilePath()
}

func (f *File) SetStorageFilePath(filePath string) {
	f.Storage.SetFilePath(filePath)
}

func (f *File) SetDefaultBaseDIR(baseDIR string) (err error) {
	log.Infof("set default base directory: %s", baseDIR)

	if baseDIR == "" {
		return fmt.Errorf("can not set empty DefaultBaseDIR")
	}
	f.DefaultBaseDIR = baseDIR
	return nil
}

func (f *File) SetDefaultFileName(filename string) (err error) {
	log.Infof("set default file name: %s", filename)

	if filename == "" {
		return fmt.Errorf("can not set empty DefaultFilename")
	}
	f.DefaultFilename = filename
	return nil
}

// List
func (f *File) List(ctx context.Context, utils utils.Utils) (err error) {
	if f.ExpireMethod == "by_duration" {
		log.Infof("listing expire(%s, before %s) files on '%s'",
			durafmt.Parse(f.ExpireDuration).String(),
			time.Now().Add(-f.ExpireDuration).Local().Format(time.RFC3339),
			f.Storage.GetType())
		f.ExpireFiles, err = (f.Storage).ListExpiredByDuration(f.ExpireDuration)
		if err != nil {
			return err
		}
	} else if f.ExpireMethod == "by_datetime" {
		log.Infof(
			"listing expire(before %s) files on %s",
			f.ExpireDatetime.Time.Local().Format(time.RFC3339),
			f.Storage.GetType(),
		)
		f.ExpireFiles, err = (f.Storage).ListExpiredByDatetime(f.ExpireDatetime)
		if err != nil {
			return err
		}
	} else if f.ExpireMethod == "" {
		log.Infof(
			"listing all files on %s",
			f.Storage.GetType(),
		)
		f.ListAllFiles, err = (f.Storage).ListExpiredByDuration(0)
		if err != nil {
			return err
		}
	}

	return nil
}

// delete
func (f *File) DeleteFiles() (err error) {
	// deleting expired files
	if f.ExpireMethod == "by_duration" {
		log.Infof(
			"deleting(%s) expire(%s, before %s) files on %s",
			f.ExpireMethod,
			durafmt.Parse(f.ExpireDuration).String(),
			time.Now().Add(-f.ExpireDuration).Format(time.RFC3339),
			f.Storage.GetType(),
		)
		f.ExpireFiles, err = (f.Storage).DeleteExpiredByDuration(f.ExpireDuration)
		if err != nil {
			return err
		}
	} else if f.ExpireMethod == "by_datetime" {
		log.Infof(
			"deleting(%s) expire(before %s) files on %s",
			f.ExpireMethod,
			f.ExpireDatetime.Time.Format(time.RFC3339),
			f.Storage.GetType(),
		)
		f.ExpireFiles, err = (f.Storage).DeleteExpiredByDatetime(f.ExpireDatetime)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupport expire method(%s)", f.ExpireMethod)
	}

	if len(f.ExpireFiles) == 0 {
		log.Infof("no expired files match prefixes")
	}

	return nil
}

func (f *File) SetMetaDB(MetaDB *gorm.DB) (err error) {
	f.MetaDB = MetaDB
	return nil
}
