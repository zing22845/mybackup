package object

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"mybackup/model"
	"mybackup/storage"
	"mybackup/utils"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hako/durafmt"
	"github.com/klauspost/pgzip"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Snapshot MySQL physical backup result
type Binlog struct {
	model.MySQLBinlogBackup
	Type            string          `json:"type,omitempty"`
	StorageID       string          `json:"storage_id,omitempty"`
	ExpireFiles     []storage.File  `json:"expire_files,omitempty"`
	ListAllFiles    []storage.File  `json:"list_all_files,omitempty"`
	Storage         storage.Storage `json:"-"`
	DefaultFilename string          `json:"-"`
	DefaultBaseDIR  string          `json:"-"`
	MetaDB          *gorm.DB        `json:"-"`
}

// user defined unmarshall method
// nolint:gocyclo
func (b *Binlog) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &b.Type)
			if err != nil {
				return err
			}
		case "storage_id":
			err = json.Unmarshal(v, &b.StorageID)
			if err != nil {
				return err
			}
		case "storage":
			b.Storage, err = storage.UnmarshalStorage(v)
			if err != nil {
				return err
			}
		case "expire_method":
			continue
		case "expire_datetime":
			t := time.Time{}
			err = json.Unmarshal(v, &t)
			if err != nil {
				return err
			}
			b.ExpireDatetime = utils.NewTime(t)
		case "expire_duration":
			tmpStr := ""
			err = json.Unmarshal(v, &tmpStr)
			if err != nil {
				return err
			}
			b.ExpireDuration, err = time.ParseDuration(tmpStr)
			if err != nil {
				return err
			}
		case "origin_size":
			err = json.Unmarshal(v, &b.OriginSize)
			if err != nil {
				return err
			}
		case "size":
			err = json.Unmarshal(v, &b.Size)
			if err != nil {
				return err
			}
		case "pack_method":
			err = json.Unmarshal(v, &b.PackMethod)
			if err != nil {
				return err
			}
		case "unpack_method":
			err = json.Unmarshal(v, &b.UnpackMethod)
			if err != nil {
				return err
			}
		case "compress_method":
			err = json.Unmarshal(v, &b.CompressMethod)
			if err != nil {
				return err
			}
		case "decompress_method":
			err = json.Unmarshal(v, &b.DecompressMethod)
			if err != nil {
				return err
			}
		case "encrypt_method":
			err = json.Unmarshal(v, &b.EncryptMethod)
			if err != nil {
				return err
			}
		case "encrypt_key":
			err = json.Unmarshal(v, &b.EncryptKey)
			if err != nil {
				return err
			}
		case "decrypt_method":
			err = json.Unmarshal(v, &b.DecryptMethod)
			if err != nil {
				return err
			}
		case "decrypt_key":
			err = json.Unmarshal(v, &b.DecryptKey)
			if err != nil {
				return err
			}
		case "md5_sum":
			err = json.Unmarshal(v, &b.MD5Sum)
			if err != nil {
				return err
			}
		case "base_dir":
			err = json.Unmarshal(v, &b.BaseDIR)
			if err != nil {
				return err
			}
		case "speed_limit":
			err = json.Unmarshal(v, &b.SpeedLimit)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	if b.ExpireMethod == "by_datetime" && b.ExpireDatetime == nil {
		return fmt.Errorf("no expire_datetime specified")
	} else if b.ExpireMethod == "by_duration" && b.ExpireDuration <= 0 {
		return fmt.Errorf(
			"invalid expire duration: %s",
			durafmt.Parse(b.ExpireDuration).String(),
		)
	}
	b.Timers = make(map[string]*utils.Timer)
	return nil
}

func (b *Binlog) GetType() string {
	return b.Type
}

// Move
func (b *Binlog) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFile, ok := (targetObject).(*Binlog)
	if !ok {
		return fmt.Errorf("only support Binlog as target now")
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

	err = b.Upload(ctx, u, targetObject, overwrite)
	if err != nil {
		return err
	}

	err = b.Storage.DeleteFile()
	if err != nil {
		return err
	}

	return err
}

// Upload
// nolint:gocyclo
func (b *Binlog) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFile, ok := (targetObject).(*Binlog)
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
	sourceFilePath := b.Storage.GetFilePath()
	if sourceFilePath != "-" {
		sourceFileInfo, err := b.Storage.GetFileInfo()
		if err != nil {
			return err
		}
		targetFile.OriginFileType = sourceFileInfo.FileType
		targetFile.OriginSize = sourceFileInfo.Size
		log.Infof("from %s filepath: %s", b.Storage.GetType(), sourceFilePath)
	} else {
		targetFile.OriginFileType = "stdin"
		targetFile.OriginSize = b.Size
		log.Infof("from stdin")
	}
	log.Infof("source file type: %s", targetFile.OriginFileType)
	log.Infof("source file size: %d", targetFile.OriginSize)

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

	// set target file path
	targetFilePath := targetFile.Storage.GetFilePath()
	if strings.HasSuffix(targetFilePath, "/") {
		targetFile.Storage.SetPrefix(targetFilePath)
		targetFile.Storage.SetFilePath(
			fmt.Sprintf("%s%s%s",
				targetFilePath,
				targetFile.DefaultFilename,
				targetFileSuffix,
			),
		)
	}
	if targetFilePath == "" {
		targetPrefix := targetFile.Storage.GetPrefix()
		if targetPrefix == "" {
			return fmt.Errorf(
				"neither FilePath nor Prefix was specified",
			)
		}
		targetFile.Storage.SetFilePath(
			fmt.Sprintf("%s%s%s",
				targetPrefix,
				targetFile.DefaultFilename,
				targetFileSuffix,
			),
		)
	}
	targetFilePath = targetFile.Storage.GetFilePath()
	log.Infof(
		"to %s filepath: %s",
		targetFile.Storage.GetType(),
		targetFilePath,
	)

	targetFileInfo, err := targetFile.Storage.GetFileInfo()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("get file(%s) stat failed", targetFilePath))
	}

	if targetFileInfo.FileStatus == "existent" {
		// check target file existent
		if !overwrite {
			return errors.Wrap(os.ErrExist, fmt.Sprintf("upload to '%s' failed", targetFilePath))
		}
		err = targetFile.Storage.DeleteFile()
		if err != nil {
			return err
		}
	}

	// prepare for upload
	switch t := targetFile.Storage.(type) {
	case *storage.FS:
		log.Debugf("backup base directory: %s", filepath.Dir(t.FilePath))
	case *storage.S3:
		uploadSize := storage.S3_MAX_UPLOAD_SIZE
		if targetFile.OriginFileType != "stdin" {
			uploadSize = targetFile.OriginSize
		}

		s3PartSize := int64(math.Ceil(float64(uploadSize) / (s3manager.MaxUploadParts - 5)))
		if s3PartSize > storage.S3_MAX_PART_SIZE {
			s3PartSize = storage.S3_MAX_PART_SIZE
		}
		concurrency := int(uploadSize / s3manager.MinUploadPartSize)
		err = t.InitS3Parameter(s3PartSize, concurrency)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("storage type is not support")
	}

	// create a io pipe
	pipeOut, pipeIn := io.Pipe()
	// done by context
	go func() {
		defer pipeIn.Close()
		<-ctx.Done()
		log.Infof("context closed")
	}()
	defer pipeOut.Close()

	var w io.Writer
	w = pipeIn

	// encrypt
	if targetFile.EncryptMethod != "" {
		if !utils.Contains(utils.SupportEncryptMethod, targetFile.EncryptMethod) {
			return fmt.Errorf(
				"unsupport encrypt method: %s",
				targetFile.EncryptMethod,
			)
		}
		if len(targetFile.EncryptKey) != 32 {
			return fmt.Errorf(
				"invalid encrypt key length: %d",
				len(targetFile.EncryptKey),
			)
		}
		w, err = utils.CryptedWriter(targetFile.EncryptKey, w)
		if err != nil {
			return fmt.Errorf(
				"encrypt stream failed: %w",
				err,
			)
		}
		log.Infof("encrypt method: %s, encrypt key: %s", targetFile.EncryptMethod, targetFile.EncryptKey)
	}

	// compress
	var compressedWriter *pgzip.Writer
	if targetFile.CompressMethod == "gzip" {
		if targetFile.OriginFileType != "application/x-gzip" {
			compressedWriter = pgzip.NewWriter(w)
			err = compressedWriter.SetConcurrency(128*1024, 8)
			if err != nil {
				return err
			}
			w = compressedWriter
			log.Infof("compressing with pgzip")
		} else {
			log.Warnf("source file is a gzip file no need to gzip again")
		}
	}

	// TODO: upload directory without tar
	if targetFile.OriginFileType == "directory" &&
		targetFile.PackMethod != "tar" {
		return fmt.Errorf("only support pack_method tar to upload directory now")
	}

	// limit speed
	if b.SpeedLimit == "" {
		b.SpeedLimit = "80M"
		log.Infof(
			"the speed limit of source is default %s/s",
			b.SpeedLimit,
		)
	} else {
		log.Infof(
			"the speed limit of source is %s/s",
			b.SpeedLimit,
		)
	}
	limitedWriter, err := utils.LimitWriter(w, b.SpeedLimit)
	if err != nil {
		return fmt.Errorf("limit writer speed to %s failed: %w",
			b.SpeedLimit, err)
	}

	// create progress writer source => progressWriter => ratelimit writer => [compress writer] => [CryptedWriter] => pipe => streamReader => storage
	//                                                                                                                                   | => hash
	progressWriter := utils.NewProgressWriter(ctx, limitedWriter, targetFile.OriginName, targetFile.OriginSize, uploadTimer, 5*time.Second)

	var wg sync.WaitGroup

	// read from source and write to limited writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer pipeIn.Close()
		if compressedWriter != nil {
			defer compressedWriter.Close()
		}
		targetFile.OriginSize, err = (b.Storage).WriteTo(progressWriter)
		if err != nil {
			b.Error = err.Error()
		}
	}()

	// init md5 writer
	hash := md5.New()
	// tee from r to md5 writer and stream reader
	streamReader := io.TeeReader(pipeOut, hash)

	// generate target to storage
	targetFile.Size, err = (targetFile.Storage).ReadFrom(streamReader)
	if err != nil {
		return fmt.Errorf("write target file failed: %w", err)
	}
	if b.Error != "" {
		return fmt.Errorf(b.Error)
	}

	// get md5sum
	targetFile.MD5Sum = fmt.Sprintf("%x", hash.Sum(nil))
	log.Infof("the md5sum of uploaded file is %s", targetFile.MD5Sum)
	log.Infof("the size of original files is %d", targetFile.OriginSize)
	log.Infof("the size of uploaded file is %d", targetFile.Size)

	return nil
}

// Download
// nolint:gocyclo
func (b *Binlog) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFile, ok := (targetObject).(*File)
	if !ok {
		return fmt.Errorf("only support file as download target now")
	}
	targetStorage, ok := (targetFile.Storage).(*storage.FS)
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
	sourceFilePath := b.Storage.GetFilePath()
	if sourceFilePath == "" {
		return fmt.Errorf("unsupport empty source FilePath on %s", b.Storage.GetType())
	}
	// check source file existence
	sourceFileInfo, err := b.Storage.GetFileInfo()
	if err != nil {
		return fmt.Errorf("get file(%s) stat failed: %w", sourceFilePath, err)
	}
	if sourceFileInfo.FileStatus == "non-existent" {
		return fmt.Errorf("source file(%s) does not exist", sourceFilePath)
	}

	// get source file info
	b.FileType = sourceFileInfo.FileType
	b.Size = sourceFileInfo.Size
	log.Infof("from %s filepath: %s", b.Storage.GetType(), sourceFilePath)
	log.Infof("source file type: %s", b.FileType)
	log.Infof("source file size: %d", b.Size)

	// check target file
	targetFilePath := targetFile.Storage.GetFilePath()
	if targetFilePath == "" {
		// check target prefix
		targetPrefix := targetFile.Storage.GetPrefix()
		if targetPrefix == "" {
			return fmt.Errorf(
				"neither FilePath nor Prefix of targetFile was specified",
			)
		}
		targetFilename := filepath.Base(sourceFilePath)
		if targetFilename == "" {
			return fmt.Errorf("empty target file name")
		}
		targetFilePath = fmt.Sprintf("%s-%d",
			filepath.Join(targetPrefix, targetFilename),
			os.Getpid(),
		)
		targetFile.Storage.SetFilePath(targetFilePath)
	}
	log.Infof("to %s filepath: %s", targetFile.Storage.GetType(), targetFilePath)

	// get target file info
	targetFileInfo, err := targetFile.Storage.GetFileInfo()
	if err != nil {
		return fmt.Errorf("get file(%s) info failed: %w", targetFilePath, err)
	}
	if targetFileInfo.FileStatus == "existent" {
		if !overwrite {
			return fmt.Errorf("target file(%s) exist", targetFilePath)
		}
		err = targetFile.Storage.DeleteFile()
		if err != nil {
			return err
		}
	}

	// create a io pipe
	pipeOut, pipeIn := io.Pipe()
	defer pipeOut.Close()
	// done by context
	go func() {
		defer pipeIn.Close()
		<-ctx.Done()
		log.Infof("context closed")
	}()
	// init md5 writer
	hash := md5.New()

	// multi write stream writer to w and hash
	streamWriter := io.MultiWriter(pipeIn, hash)

	// read from source and write to stream writer
	go func() {
		defer pipeIn.Close()
		targetFile.OriginSize, err = (b.Storage).WriteTo(streamWriter)
		if err != nil {
			b.Error = err.Error()
		}
	}()

	var r io.Reader

	r = pipeOut
	// encrypt or decrypt stream
	if targetFile.DecryptMethod != "" {
		if !utils.Contains(utils.SupportEncryptMethod, targetFile.DecryptMethod) {
			return fmt.Errorf(
				"unsupport encrypt method: %s",
				targetFile.DecryptMethod,
			)
		}
		if len(targetFile.DecryptKey) != 32 {
			return fmt.Errorf(
				"invalid decrypt key length: %d",
				len(targetFile.DecryptKey),
			)
		}
		r, err = utils.CryptedReader(targetFile.DecryptKey, r)
		if err != nil {
			return fmt.Errorf("get decrypt reader faild: %w", err)
		}
		log.Infof("decrypt method: %s, decrypt key: %s", targetFile.DecryptMethod, targetFile.DecryptKey)
	}

	// decompress
	targetFile.Size = sourceFileInfo.Size
	if targetFile.DecompressMethod == "gzip" {
		if b.FileType != "application/x-gzip" && targetFile.DecryptMethod == "" {
			return fmt.Errorf("the source file is not a gzip file")
		}
		r, err = pgzip.NewReaderN(r, 128*1024, 8)
		if err != nil {
			return fmt.Errorf("create decompress reader failed: %w", err)
		}
		if targetFile.DecryptMethod == "" && sourceFileInfo.UncompressedSize > 0 {
			targetFile.Size = sourceFileInfo.UncompressedSize
		}
		log.Infof("decompressing with pgzip")
	}

	// create progress reader: storage => streamWriter => pipe =>
	//                                               | => hash
	//   [CryptedReader] => [decompress reader] => progressReader => ratelimit reader => target[file/untar dir]
	targetFileName := filepath.Base(targetFilePath)
	progressReader := utils.NewProgressReader(ctx, r, targetFileName, targetFile.Size, downloadTimer, 5*time.Second)

	// limit speed
	if targetFile.SpeedLimit == "" {
		targetFile.SpeedLimit = "160M"
		log.Infof(
			"the speed limit of downloading is default %s/s",
			targetFile.SpeedLimit,
		)
	} else {
		log.Infof(
			"the speed limit of downloading is %s/s",
			targetFile.SpeedLimit,
		)
	}
	limitedReader, err := utils.LimitReader(progressReader, targetFile.SpeedLimit)
	if err != nil {
		return fmt.Errorf("limit reader speed to %s failed: %w",
			targetFile.SpeedLimit, err)
	}

	// generate target to storage
	if targetFile.UnpackMethod == "tar" {
		targetStorage.FileType = "directory"
	}
	targetFile.Size, err = (targetFile.Storage).ReadFrom(limitedReader)
	if err != nil {
		return fmt.Errorf("read data from limited reader failed: %w", err)
	}
	if b.Error != "" {
		return fmt.Errorf(b.Error)
	}

	// get md5sum and compare with origin
	targetFile.MD5Sum = fmt.Sprintf("%x", hash.Sum(nil))
	if b.MD5Sum != "" {
		if b.MD5Sum != targetFile.MD5Sum {
			return fmt.Errorf(
				"source md5sum(%s) != target md5sum(%s), checksum mismatch",
				b.MD5Sum, targetFile.MD5Sum,
			)
		}
		log.Infof("the md5sum of source file is %s", b.MD5Sum)
	}
	log.Infof("the md5sum of target file is %s", targetFile.MD5Sum)
	return err
}

// Backup
func (b *Binlog) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFile, ok := (targetObject).(*File)
	if !ok {
		return fmt.Errorf("only support File as target now")
	}
	// calculate time
	backupTimer := utils.NewTimer()
	targetFile.Timers["backup"] = backupTimer
	defer func() {
		backupTimer.Stop()
		log.Infof("duration of backup: %s",
			backupTimer.Duration.String(),
		)
	}()
	err = b.Upload(ctx, u, targetObject, overwrite)
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
func (b *Binlog) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	// check type
	targetFile, ok := (targetObject).(*File)
	if !ok {
		return fmt.Errorf("only support File as target now")
	}
	// calculate time
	recoverTimer := utils.NewTimer()
	targetFile.Timers["recover"] = recoverTimer
	defer func() {
		recoverTimer.Stop()
		log.Infof("duration of total recovery: %s",
			recoverTimer.Duration.String(),
		)
	}()
	// download
	err = b.Download(ctx, u, targetObject, overwrite)
	if err != nil {
		return err
	}
	return err
}

func (b *Binlog) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("not support yet")
}

func (b *Binlog) GetStorageFilePath() string {
	return b.Storage.GetFilePath()
}

func (b *Binlog) SetStorageFilePath(filePath string) {
	b.Storage.SetFilePath(filePath)
}

func (b *Binlog) SetDefaultBaseDIR(baseDIR string) (err error) {
	log.Infof("set default base directory: %s", baseDIR)

	if baseDIR == "" {
		return fmt.Errorf("can not set empty DefaultBaseDIR")
	}
	b.DefaultBaseDIR = baseDIR
	return nil
}

func (b *Binlog) SetDefaultFileName(filename string) (err error) {
	log.Infof("set default file name: %s", filename)

	if filename == "" {
		return fmt.Errorf("can not set empty DefaultFilename")
	}
	b.DefaultFilename = filename
	return nil
}

// List
func (b *Binlog) List(ctx context.Context, utils utils.Utils) (err error) {
	if b.ExpireMethod == "by_duration" {
		log.Infof("listing expire(%s, before %s) files on '%s'",
			durafmt.Parse(b.ExpireDuration).String(),
			time.Now().Add(-b.ExpireDuration).Local().Format(time.RFC3339),
			b.Storage.GetType())
		b.ExpireFiles, err = (b.Storage).ListExpiredByDuration(b.ExpireDuration)
		if err != nil {
			return err
		}
	} else if b.ExpireMethod == "by_datetime" {
		log.Infof(
			"listing expire(before %s) files on %s",
			b.ExpireDatetime.Time.Local().Format(time.RFC3339),
			b.Storage.GetType(),
		)
		b.ExpireFiles, err = (b.Storage).ListExpiredByDatetime(b.ExpireDatetime)
		if err != nil {
			return err
		}
	} else if b.ExpireMethod == "" {
		log.Infof(
			"listing all files on %s",
			b.Storage.GetType(),
		)
		b.ListAllFiles, err = (b.Storage).ListExpiredByDuration(0)
		if err != nil {
			return err
		}
	}

	return nil
}

// delete
func (b *Binlog) DeleteFiles() (err error) {
	// deleting expired files
	if b.ExpireMethod == "by_duration" {
		log.Infof(
			"deleting(%s) expire(%s, before %s) files on %s",
			b.ExpireMethod,
			durafmt.Parse(b.ExpireDuration).String(),
			time.Now().Add(-b.ExpireDuration).Format(time.RFC3339),
			b.Storage.GetType(),
		)
		b.ExpireFiles, err = (b.Storage).DeleteExpiredByDuration(b.ExpireDuration)
		if err != nil {
			return err
		}
	} else if b.ExpireMethod == "by_datetime" {
		log.Infof(
			"deleting(%s) expire(before %s) files on %s",
			b.ExpireMethod,
			b.ExpireDatetime.Time.Format(time.RFC3339),
			b.Storage.GetType(),
		)
		b.ExpireFiles, err = (b.Storage).DeleteExpiredByDatetime(b.ExpireDatetime)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupport expire method(%s)", b.ExpireMethod)
	}

	if len(b.ExpireFiles) == 0 {
		log.Infof("no expired files match prefixes")
	}

	return nil
}

func (b *Binlog) SetMetaDB(MetaDB *gorm.DB) (err error) {
	b.MetaDB = MetaDB
	return nil
}
