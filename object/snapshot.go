package object

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"mybackup/model"
	"mybackup/storage"
	"mybackup/utils"

	"github.com/dustin/go-humanize"
	"github.com/hako/durafmt"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Snapshot MySQL physical backup result
type Snapshot struct {
	model.MySQLPhysicalBackup
	Type             string          `json:"type,omitempty"`
	StorageID        string          `json:"storage_id,omitempty"`
	IncrementalLevel int16           `json:"incremental_level,omitempty"`
	IncrementalDIR   string          `json:"incremental_dir,omitempty"`
	Export           bool            `json:"export,omitempty"`
	ExpireFiles      []storage.File  `json:"expire_files,omitempty"`
	ListAllFiles     []storage.File  `json:"list_all_files,omitempty"`
	Storage          storage.Storage `json:"storage,omitempty"`
	Stream           *Stream         `json:"-"`
	DefaultBaseDIR   string          `json:"-"`
	DefaultFilename  string          `json:"-"`
	Filename         string          `json:"-"`
	MetaDB           *gorm.DB        `json:"-"`
}

// user defined unmarshall method
func (s *Snapshot) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &s.Type)
			if err != nil {
				return err
			}
		case "storage_id":
			err = json.Unmarshal(v, &s.StorageID)
			if err != nil {
				return err
			}
		case "storage":
			s.Storage, err = storage.UnmarshalStorage(v)
			if err != nil {
				return err
			}
		case "incremental_level":
			err = json.Unmarshal(v, &s.IncrementalLevel)
			if err != nil {
				return err
			}
		case "from_lsn":
			err = json.Unmarshal(v, &s.FromLSN)
			if err != nil {
				return err
			}
		case "to_lsn":
			err = json.Unmarshal(v, &s.ToLSN)
			if err != nil {
				return err
			}
		case "mysql_version":
			err = json.Unmarshal(v, &s.MySQLVersion)
			if err != nil {
				return err
			}
		case "expire_method":
			err = json.Unmarshal(v, &s.ExpireMethod)
			if err != nil {
				return err
			}
			if !utils.Contains(storage.SupportExpireMethod, s.ExpireMethod) {
				return fmt.Errorf("unsupport expired method: %s", s.ExpireMethod)
			}
		case "expire_datetime":
			t := time.Time{}
			err = json.Unmarshal(v, &t)
			if err != nil {
				return err
			}
			s.ExpireDatetime = utils.NewTime(t)
		case "expire_duration":
			tmpStr := ""
			err = json.Unmarshal(v, &tmpStr)
			if err != nil {
				return err
			}
			s.ExpireDuration, err = time.ParseDuration(tmpStr)
			if err != nil {
				return err
			}
		case "origin_size":
			err = json.Unmarshal(v, &s.OriginSize)
			if err != nil {
				return err
			}
		case "export":
			err = json.Unmarshal(v, &s.Export)
			if err != nil {
				return err
			}
		case "size":
			err = json.Unmarshal(v, &s.Size)
			if err != nil {
				return err
			}
		case "pack_method":
			err = json.Unmarshal(v, &s.PackMethod)
			if err != nil {
				return err
			}
		case "unpack_method":
			err = json.Unmarshal(v, &s.UnpackMethod)
			if err != nil {
				return err
			}
		case "compress_method":
			err = json.Unmarshal(v, &s.CompressMethod)
			if err != nil {
				return err
			}
		case "decompress_method":
			err = json.Unmarshal(v, &s.DecompressMethod)
			if err != nil {
				return err
			}
		case "encrypt_method":
			err = json.Unmarshal(v, &s.EncryptMethod)
			if err != nil {
				return err
			}
		case "encrypt_key":
			err = json.Unmarshal(v, &s.EncryptKey)
			if err != nil {
				return err
			}
		case "decrypt_method":
			err = json.Unmarshal(v, &s.DecryptMethod)
			if err != nil {
				return err
			}
		case "decrypt_key":
			err = json.Unmarshal(v, &s.DecryptKey)
			if err != nil {
				return err
			}
		case "md5_sum":
			err = json.Unmarshal(v, &s.MD5Sum)
			if err != nil {
				return err
			}
		case "base_dir":
			err = json.Unmarshal(v, &s.BaseDIR)
			if err != nil {
				return err
			}
		case "speed_limit":
			err = json.Unmarshal(v, &s.SpeedLimit)
			if err != nil {
				return err
			}
		case "databases":
			err = json.Unmarshal(v, &s.Databases)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	if s.ExpireMethod == "by_datetime" && s.ExpireDatetime == nil {
		return fmt.Errorf("no expire_datetime specified")
	} else if s.ExpireMethod == "by_duration" && s.ExpireDuration <= 0 {
		return fmt.Errorf(
			"invalid expire duration: %s",
			durafmt.Parse(s.ExpireDuration).String(),
		)
	}
	s.Timers = make(map[string]*utils.Timer)
	return nil
}

// Move
func (s *Snapshot) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport action")
}

func (s *Snapshot) GetType() string {
	return s.Type
}

// Upload
func (s *Snapshot) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport action")
}

func (s *Snapshot) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	err = s.prepareSource(ctx)
	if err != nil {
		return err
	}
	switch t := (targetObject).(type) {
	case *Snapshot:
		t.Err = s.downloadToSingle(ctx, u, t, overwrite)
		if t.Err == nil {
			t.Status = "success"
		}
		return t.WriteMeta()
	case *SnapshotSet:
		t.Err = s.downloadToMulti(ctx, u, t, overwrite)
		return t.WriteMeta()
	default:
		return fmt.Errorf("unsupport target type")
	}
}

// Download snapshot to snapshot set
func (s *Snapshot) downloadToMulti(ctx context.Context, u utils.Utils, target *SnapshotSet, overwrite bool) (err error) {
	if len(target.SnapshotSet) == 0 {
		return fmt.Errorf("empty target snapshot set")
	}
	for n, targetSnapshot := range target.SnapshotSet {
		log.Infof("snapshot[%d]: preparing download to %s",
			n, targetSnapshot.Storage.GetType())
		// defer stop timer and write meta
		defer func(s *Snapshot) {
			s.Timers["download"].Stop()
			log.Infof("snapshot[%d]: duration of download: %s",
				n, s.Timers["download"].Duration.String())
			s.DownloadEndTime =
				s.Timers["download"].StopTime
			if err != nil {
				s.Err = err
			} else {
				s.Status = "success_download"
			}
			errMeta := s.WriteMeta()
			if errMeta != nil && err == nil {
				err = errMeta
			}
		}(targetSnapshot)
		// prepare download stream
		err = s.prepareSingleDownload(ctx, u, targetSnapshot, overwrite)
		if err != nil {
			return fmt.Errorf("snapshot[%d] prepare single download to %s failed: %w",
				n, targetSnapshot.Storage.GetType(), err)
		}
		// add stream writers
		target.InputStreamsWriters = append(
			target.InputStreamsWriters,
			targetSnapshot.Stream.InputWriter)
	}
	// combile stream writers
	target.InputMultiWriter = io.MultiWriter(target.InputStreamsWriters...)
	// read from source storage and write to combined multiwriter
	var size int64
	size, target.Err = (s.Storage).WriteTo(target.InputMultiWriter)
	// done stream and check result
	for n, targetSnapshot := range target.SnapshotSet {
		targetSnapshot.Size = size
		if target.Err != nil {
			targetSnapshot.Err = target.Err
			continue
		}
		// cancel and wait stream done
		targetSnapshot.Stream.Cancel()
		targetSnapshot.Stream.WaitGroup.Wait()
		if targetSnapshot.Err == nil && targetSnapshot.Stream.Err != nil {
			targetSnapshot.Err = targetSnapshot.Stream.Err
			target.Err = targetSnapshot.Err
			continue
		}
		// set size
		targetSnapshot.Size = targetSnapshot.Stream.OutputSize
		// get md5sum and compare with origin
		targetSnapshot.MD5Sum = targetSnapshot.Stream.OutputChecksum
		log.Infof("snapshot[%d]: wait download complete", n)
	}
	return target.Err
}

// Download snapshot to single snapshot
func (s *Snapshot) downloadToSingle(ctx context.Context, u utils.Utils, target *Snapshot, overwrite bool) (err error) {
	// defer stop timer and write meta
	defer func(s *Snapshot) {
		s.Timers["download"].Stop()
		log.Infof("duration of download: %s", s.Timers["download"].Duration.String())
		s.DownloadEndTime = s.Timers["download"].StopTime
		if err != nil {
			s.Err = err
		} else {
			s.Status = "success_download"
		}
		errMeta := s.WriteMeta()
		if errMeta != nil && err == nil {
			err = errMeta
		}
	}(target)
	// prepare target snapshot
	err = s.prepareSingleDownload(ctx, u, target, overwrite)
	if err != nil {
		return err
	}
	target.Stream.ExpectInputChecksum = s.MD5Sum
	// read from source storage and write to target stream
	target.Size, err = (s.Storage).WriteTo(target.Stream.InputWriter)
	if err != nil {
		target.Err = err
		return err
	}
	// cancel and wait stream done
	target.Stream.Cancel()
	target.Stream.WaitGroup.Wait()
	if target.Stream.Err != nil {
		return target.Stream.Err
	}
	// get target md5sum
	target.MD5Sum = target.Stream.OutputChecksum
	return err
}

func (s *Snapshot) prepareSource(ctx context.Context) (err error) {
	// prepare source storage
	switch st := s.Storage.(type) {
	case *storage.S3, *storage.FS:
		// check source file
		sourceFilePath := s.Storage.GetFilePath()
		if sourceFilePath == "" {
			return fmt.Errorf("unsupport empty source FilePath on %s", s.Storage.GetType())
		}
		sourceFileInfo, err := s.Storage.GetFileInfo()
		if err != nil {
			return err
		}
		s.Size = sourceFileInfo.Size
		s.Filename = filepath.Base(sourceFilePath)
		if s.Filename == "" {
			return fmt.Errorf("empty source file name")
		}
		log.Infof("from %s filepath: %s", s.Storage.GetType(), sourceFilePath)
	case *storage.TCP:
		if st.Conn == nil {
			// listening on port
			listen, err := st.InitServerListener(ctx)
			if err != nil {
				log.Errorf("init tcp server listener failed: %+v", err)
				return err
			}
			// init metadb
			s.Err = s.MetaDB.AutoMigrate(&model.MySQLPhysicalBackup{})
			if s.Err != nil {
				return s.Err
			}
			// prepare metadata
			s.Err = s.prepareMetaData("listening")
			if s.Err != nil {
				return s.Err
			}
			// block and wait connection
			err = st.InitServerConn(ctx, listen)
			if err != nil {
				log.Errorf("init tcp server connection failed: %+v", err)
				return err
			}
			s.Status = "connected"
			s.Err = s.WriteMeta()
			if s.Err != nil {
				log.Errorf("write meta after connecting failed: %+v", s.Err)
				return s.Err
			}
			s.Filename = fmt.Sprintf("%s_%s_%d", st.Type, st.IP, st.Port)
			s.Size = -1
			log.Infof("from %s: %s", st.Type, st.Conn.LocalAddr())
		}
	}
	return nil
}

func (s *Snapshot) prepareSingleDownload(ctx context.Context, u utils.Utils, target *Snapshot, overwrite bool) (err error) {
	// calculate time
	target.Timers["download"] = utils.NewTimer()
	// init metadb
	target.Err = target.MetaDB.AutoMigrate(&model.MySQLPhysicalBackup{})
	if target.Err != nil {
		return target.Err
	}
	// prepare metadata
	if target.DatetimeStart == nil {
		target.DatetimeStart = target.Timers["download"].StartTime
	}
	target.DownloadStartTime = target.Timers["download"].StartTime
	target.Err = target.prepareMetaData("creating")
	if target.Err != nil {
		return target.Err
	}
	// set size
	target.OriginSize = s.Size
	// set default file name
	target.DefaultFilename = fmt.Sprintf("%s-%d", filepath.Base(s.Storage.GetFilePath()), target.ID)
	// create target stream
	target.Stream = NewStream(ctx, 5*time.Second, s.Filename)
	target.Stream.ExpectInputChecksum = s.MD5Sum
	target.Stream.InputSize = s.Size
	target.Stream.OutputSize = s.OriginSize
	target.Stream.InputSpeedLimit = s.SpeedLimit
	target.Stream.OutputSpeedLimit = target.SpeedLimit
	target.Stream.CompressMethod = s.CompressMethod
	target.Stream.DecompressMethod = target.DecompressMethod
	target.Stream.UnpackMethod = target.UnpackMethod
	// store stream
	return target.Stream.Store(u, target.Storage, target.DefaultFilename, overwrite)
}

// Backup backup Snapshot to another Object
func (s *Snapshot) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport operation now")
}

// Recover recover from Snapshot to another snapshot
func (s *Snapshot) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	err = s.prepareSource(ctx)
	if err != nil {
		return err
	}
	switch t := (targetObject).(type) {
	case *Snapshot:
		// calculate time
		t.Timers["recover"] = utils.NewTimer()
		t.DatetimeStart = t.Timers["recover"].StartTime
		t.RecoverStartTime = t.Timers["recover"].StartTime
		defer func(s *Snapshot) {
			s.Timers["recover"].Stop()
			log.Infof("duration recovery: %s", s.Timers["recover"].Duration.String())
			s.RecoverEndTime = s.Timers["recover"].StopTime
			if s.Err == nil {
				s.Status = "success"
			}
			errMeta := s.WriteMeta()
			if errMeta != nil && err == nil {
				err = errMeta
			}
		}(t)
		// download to single snapshot
		t.Err = s.downloadToSingle(ctx, u, t, overwrite)
		if t.Err != nil {
			return t.Err
		}
		// prepare data
		t.Err = t.Prepare(ctx, u)
		if t.Err != nil {
			return t.Err
		}
	case *SnapshotSet:
		// calculate time
		t.Timers["recover"] = utils.NewTimer()
		defer func(ss *SnapshotSet) {
			ss.Timers["recover"].Stop()
			log.Infof("duration recovery: %s",
				ss.Timers["recover"].Duration.String(),
			)
			if ss.Err == nil {
				ss.Status = "success"
			}
			errMeta := ss.WriteMeta()
			if errMeta != nil && err == nil {
				err = errMeta
			}
		}(t)
		// download to snapshot set
		t.Err = s.downloadToMulti(ctx, u, t, overwrite)
		if t.Err != nil {
			return t.Err
		}
		log.Infof("download snapshot set success")
		// prepare snapshot set
		t.Err = t.prepareParallel(ctx, u)
		if t.Err != nil {
			return t.Err
		}
		log.Infof("prepare snpashot set success")
	case *MySQL:
		// calculate time
		recoverTimer := utils.NewTimer()
		t.Timers["recover"] = recoverTimer
		defer func() {
			recoverTimer.Stop()
			log.Infof("duration of total recovery: %s",
				recoverTimer.Duration.String(),
			)
		}()
		// TODO: implementing recover snapshot to MySQL
		return fmt.Errorf("incoming support")
	default:
		return fmt.Errorf("does not support recover snapshot to %s", t.GetType())
	}
	return err
}

func (s *Snapshot) Prepare(ctx context.Context, u utils.Utils) (err error) {
	// calculate time
	s.Timers["prepare"] = utils.NewTimer()
	// init metadb
	err = s.MetaDB.AutoMigrate(&model.MySQLPhysicalBackup{})
	if err != nil {
		return err
	}
	// prepare metadata
	if s.DatetimeStart == nil {
		s.DatetimeStart = s.Timers["prepare"].StartTime
	}
	s.PrepareStartTime = s.Timers["prepare"].StartTime
	err = s.prepareMetaData("creating")
	if err != nil {
		return err
	}
	// defer stop timer and write meta
	defer func(s *Snapshot) {
		s.Timers["prepare"].Stop()
		log.Infof("duration of preparing: %s", s.Timers["prepare"].Duration.String())
		s.PrepareEndTime = s.Timers["prepare"].StopTime
		if err != nil {
			s.Err = err
			s.Status = "failed"
		} else {
			s.Status = "success_prepare"
		}
		errMeta := s.WriteMeta()
		if errMeta != nil && err == nil {
			err = errMeta
		}
	}(s)
	// prepare data
	u.Xtrabackup.PrepareDataDIR = s.Storage.GetFilePath()
	log.Infof("start to prepare data files in '%s'", u.Xtrabackup.PrepareDataDIR)
	if s.BaseDIR == "" {
		// set base dir for full backup
		s.BaseDIR = u.Xtrabackup.PrepareDataDIR
	}
	// set base dir as target dir for both full backup and incremental backup
	u.Xtrabackup.TargetDIR = s.BaseDIR
	u.Xtrabackup.Action = "prepare"
	if u.Xtrabackup.UseMemory == "" {
		u.Xtrabackup.UseMemory = "500M"
	}
	u.Xtrabackup.Export = s.Export
	err = u.Xtrabackup.GenerateCMD()
	if err != nil {
		return fmt.Errorf("generate xtrabackup command failed: %w", err)
	}
	log.Infof("prepare command: %s", u.Xtrabackup.CMD.String())
	err = u.Xtrabackup.CMD.Run()
	if err != nil {
		return fmt.Errorf("xtrabackup prepare failed: %w", err)
	}
	// remove incremental dir
	if u.Xtrabackup.PrepareRemoveIncrementalDIR && u.Xtrabackup.IncrementalDIR != "" {
		err := os.RemoveAll(u.Xtrabackup.IncrementalDIR)
		if err != nil {
			return fmt.Errorf(
				"failed to remove incremental dir '%s', with error: %w",
				u.Xtrabackup.IncrementalDIR,
				err,
			)
		}
		log.Infof("removed incremental directory: %s",
			u.Xtrabackup.IncrementalDIR,
		)
	}
	// remove xtrabackup_logfile
	if u.Xtrabackup.PrepareRemoveXtrabackupLogFile && !u.Xtrabackup.ApplyLogOnly {
		xtrabackupLogfile := fmt.Sprintf("%s/xtrabackup_logfile", s.BaseDIR)
		if xtrabackupLogfileInfo, err := os.Stat(xtrabackupLogfile); err != nil {
			return fmt.Errorf(
				"try to remove xtrabackup_logfile(%s) with error: %w",
				xtrabackupLogfile,
				err,
			)
		} else {
			err := os.Remove(xtrabackupLogfile)
			if err != nil {
				return fmt.Errorf(
					"remove xtrabackup_logfile(%s) failed: %w",
					xtrabackupLogfile,
					err,
				)
			}
			log.Infof(
				"removed xtrabackup_logfile(%s) and released %s",
				xtrabackupLogfile,
				humanize.Bytes(
					uint64(xtrabackupLogfileInfo.Size()),
				),
			)
		}
	}
	return err
}

func (s *Snapshot) GetStorageFilePath() string {
	return s.Storage.GetFilePath()
}

// List
func (s *Snapshot) List(ctx context.Context, utils utils.Utils) (err error) {
	if s.ExpireMethod == "by_duration" {
		log.Infof("listing expire(%s, before %s) files on '%s'",
			durafmt.Parse(s.ExpireDuration).String(),
			time.Now().Add(-s.ExpireDuration).Local().Format(time.RFC3339),
			s.Storage.GetType())
		s.ExpireFiles, err = (s.Storage).ListExpiredByDuration(s.ExpireDuration)
		if err != nil {
			return err
		}
	} else if s.ExpireMethod == "by_datetime" {
		log.Infof(
			"listing expire(before %s) files on %s",
			s.ExpireDatetime.Time.Local().Format(time.RFC3339),
			s.Storage.GetType(),
		)
		s.ExpireFiles, err = (s.Storage).ListExpiredByDatetime(s.ExpireDatetime)
		if err != nil {
			return err
		}
	} else if s.ExpireMethod == "" {
		log.Infof(
			"listing all files on %s",
			s.Storage.GetType(),
		)
		s.ListAllFiles, err = (s.Storage).ListExpiredByDuration(0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Snapshot) SetDefaultBaseDIR(baseDIR string) (err error) {
	log.Infof("set default base directory: %s", baseDIR)
	if baseDIR == "" {
		return fmt.Errorf("can not set empty DefaultBaseDIR")
	}
	s.DefaultBaseDIR = baseDIR
	return nil
}

func (s *Snapshot) SetDefaultFileName(filename string) (err error) {
	log.Infof("set default file name: %s", filename)
	if filename == "" {
		return fmt.Errorf("can not set empty DefaultFilename")
	}
	s.DefaultFilename = filename
	return nil
}

func (s *Snapshot) SetStorageFilePath(filePath string) {
	s.Storage.SetFilePath(filePath)
}

// delete
func (s *Snapshot) DeleteFiles() (err error) {
	// deleting expired files
	if s.ExpireMethod == "by_duration" {
		log.Infof(
			"deleting(%s) expire(%s, before %s) files on %s",
			s.ExpireMethod,
			durafmt.Parse(s.ExpireDuration).String(),
			time.Now().Add(-s.ExpireDuration).Format(time.RFC3339),
			s.Storage.GetType(),
		)
		s.ExpireFiles, err = (s.Storage).DeleteExpiredByDuration(s.ExpireDuration)
		if err != nil {
			return err
		}
	} else if s.ExpireMethod == "by_datetime" {
		log.Infof(
			"deleting(%s) expire(before %s) files on %s",
			s.ExpireMethod,
			s.ExpireDatetime.Time.Format(time.RFC3339),
			s.Storage.GetType(),
		)
		s.ExpireFiles, err = (s.Storage).DeleteExpiredByDatetime(s.ExpireDatetime)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupport expire method(%s)", s.ExpireMethod)
	}

	if len(s.ExpireFiles) == 0 {
		log.Infof("no expired files match prefixes")
	}

	return nil
}

func (s *Snapshot) SetMetaDB(MetaDB *gorm.DB) (err error) {
	s.MetaDB = MetaDB
	return nil
}

func (s *Snapshot) prepareMetaData(status string) (err error) {
	// init meta data
	s.MySQLPhysicalBackup.Status = status
	switch st := s.Storage.(type) {
	case *storage.FS:
	case *storage.S3:
	case *storage.TCP:
		s.MySQLPhysicalBackup.IP = st.IP
		s.MySQLPhysicalBackup.Port = st.Port
		s.MySQLPhysicalBackup.ConnTimeout = st.ConnTimeout
	default:
		return fmt.Errorf("unsupport storage type")
	}
	return s.WriteMeta()
}

func (s *Snapshot) WriteMeta() error {
	if s.Err != nil {
		s.Status = "failed"
		s.Error = s.Err.Error()
	}
	s.MetaDB.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{
				{Name: "datetime_start"},
				{Name: "before_backup_gtid_set"},
			},
			UpdateAll: true,
		}).Save(&s.MySQLPhysicalBackup)
	if s.MetaDB.Error != nil && s.Err == nil {
		s.Err = s.MetaDB.Error
		s.Error = s.Err.Error()
	}
	return s.Err
}
