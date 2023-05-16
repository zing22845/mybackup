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

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type BinlogSet struct {
	Type                  string                     `json:"type,omitempty"`
	BinlogSet             map[string]*Binlog         `json:"snapshot_set,omitempty"`
	DefaultCompressMethod string                     `json:"default_compress_method,omitempty"`
	DefaultSpeedLimit     string                     `json:"default_speed_limit,omitempty"`
	Storages              map[string]storage.Storage `json:"storages,omitempty"`
	DefaultStorageID      string                     `json:"default_storage_id,omitempty"`
	DefaultStorage        storage.Storage            `json:"default_storage,omitempty"`
	DefaultMasterIP       string                     `json:"-"`
	DefaultMasterPort     uint16                     `json:"-"`
	MetaDB                *gorm.DB                   `json:"-"`
}

// user defined unmarshall method
func (bs *BinlogSet) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	storages := make(map[string]json.RawMessage)
	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &bs.Type)
			if err != nil {
				return err
			}
		case "binlog_set":
			err = json.Unmarshal(v, &bs.BinlogSet)
			if err != nil {
				return err
			}
		case "default_storage_id":
			err = json.Unmarshal(v, &bs.DefaultStorageID)
			if err != nil {
				return err
			}
		case "storages":
			bs.Storages = make(map[string]storage.Storage)
			err = json.Unmarshal(v, &storages)
			if err != nil {
				return err
			}
			for i, j := range storages {
				bs.Storages[i], err = storage.UnmarshalStorage(j)
				if err != nil {
					return err
				}
			}
		case "default_compress_method":
			err = json.Unmarshal(v, &bs.DefaultCompressMethod)
			if err != nil {
				return err
			}
		case "default_speed_limit":
			err = json.Unmarshal(v, &bs.DefaultSpeedLimit)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}

	var ok bool
	if bs.DefaultStorageID != "" {
		bs.DefaultStorage, ok = bs.Storages[bs.DefaultStorageID]
		if !ok {
			return fmt.Errorf("no default storage(%s) in storages", bs.DefaultStorageID)
		}
	}
	if bs.BinlogSet == nil {
		bs.BinlogSet = make(map[string]*Binlog, 0)
	}
	for name, binlog := range bs.BinlogSet {
		if binlog.Storage == nil {
			binlog.StorageID = bs.DefaultStorageID
			if binlog.StorageID == "" {
				return fmt.Errorf("neither storage nor storage id for binlog[%s]", name)
			}
			binlog.Storage, ok = bs.Storages[binlog.StorageID]
			if !ok {
				return fmt.Errorf("no storage(%s) in storages", binlog.StorageID)
			}
		}
	}
	return nil
}

func (bs *BinlogSet) GetType() string {
	return bs.Type
}

// Move
func (bs *BinlogSet) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetBinlogSet, ok := (targetObject).(*BinlogSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetBinlogSet.BinlogSet) != len(bs.BinlogSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetBinlogSet.BinlogSet), len(bs.BinlogSet))
	}

	// move each file to target
	for name, binlog := range bs.BinlogSet {
		log.Infof("moving file %s", name)
		err = binlog.Move(ctx, u, targetBinlogSet.BinlogSet[name], overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Upload
func (bs *BinlogSet) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetBinlogSet, ok := (targetObject).(*BinlogSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetBinlogSet.BinlogSet) != len(bs.BinlogSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetBinlogSet.BinlogSet), len(bs.BinlogSet))
	}

	// upload each binlog to target
	for name, binlog := range bs.BinlogSet {
		log.Infof("uploading file %s", name)
		err = binlog.Upload(ctx, u, targetBinlogSet.BinlogSet[name], overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Download
func (bs *BinlogSet) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetBinlogSet, ok := (targetObject).(*BinlogSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetBinlogSet.BinlogSet) != len(bs.BinlogSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetBinlogSet.BinlogSet), len(bs.BinlogSet))
	}
	// download each binlog to target
	for name, binlog := range bs.BinlogSet {
		log.Infof("downloading binlog %s", name)
		err = binlog.Download(ctx, u, targetObject, overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Backup backup binlog set to target place
func (bs *BinlogSet) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetBinlogSet, ok := (targetObject).(*BinlogSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetBinlogSet.BinlogSet) != len(bs.BinlogSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetBinlogSet.BinlogSet), len(bs.BinlogSet))
	}

	// backup each file to target
	for name, binlog := range bs.BinlogSet {
		log.Infof("backuping file %s", name)
		err = binlog.Backup(ctx, u, targetBinlogSet.BinlogSet[name], overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Recover recover binlog set to target place
func (bs *BinlogSet) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetBinlogSet, ok := (targetObject).(*BinlogSet)
	if !ok {
		return fmt.Errorf("only support binlog_set as target now")
	}
	if len(targetBinlogSet.BinlogSet) != len(bs.BinlogSet) {
		return fmt.Errorf("target binlog_set count(%d) not match source binlog_set count(%d)", len(targetBinlogSet.BinlogSet), len(bs.BinlogSet))
	}

	// recover each file to target
	for name, binlog := range bs.BinlogSet {
		log.Infof("recovering file[%s]: %s", name, binlog.GetStorageFilePath())
		err = binlog.Recover(ctx, u, targetBinlogSet.BinlogSet[name], overwrite)
		if err != nil {
			return err
		}
	}

	return err
}

func (bs *BinlogSet) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("not support")
}

func (bs *BinlogSet) GetStorageFilePath() string {
	log.Infof("not support yet")
	return ""
}

func (bs *BinlogSet) SetStorageFilePath(filePath string) {
	log.Infof("not support yet")
}

func (bs *BinlogSet) SetDefaultBaseDIR(baseDIR string) (err error) {
	for _, binlog := range bs.BinlogSet {
		err = binlog.SetDefaultBaseDIR(baseDIR)
		if err != nil {
			return nil
		}
	}
	return nil
}

func (bs *BinlogSet) SetDefaultFileName(filename string) (err error) {
	for _, binlog := range bs.BinlogSet {
		err = binlog.SetDefaultFileName(filename)
		if err != nil {
			return nil
		}
	}
	return nil
}

// List expired snapshot
func (bs *BinlogSet) List(ctx context.Context, utils utils.Utils) (err error) {
	return fmt.Errorf("not support")
}

func (bs *BinlogSet) NewBinlog(name string, lastBinlog *Binlog, startTimestamp uint32, binlogChecksum byte) (*Binlog, error) {
	binlog := &Binlog{
		MySQLBinlogBackup: model.MySQLBinlogBackup{},
	}
	if lastBinlog != nil {
		binlog.LastName = lastBinlog.OriginName
		binlog.LastID = lastBinlog.ID
	}
	binlog.OriginName = name
	binlog.CompressMethod = bs.DefaultCompressMethod
	binlog.BinlogChecksum = binlogChecksum
	fileEXT := ""
	switch binlog.CompressMethod {
	case "":
	case "gzip":
		fileEXT = ".gz"
	default:
		return nil, fmt.Errorf("unsupport compress method: %s", binlog.CompressMethod)
	}
	binlog.MasterIP = bs.DefaultMasterIP
	binlog.MasterPort = bs.DefaultMasterPort
	binlog.Storage = bs.DefaultStorage
	startDatetime := time.Unix(int64(startTimestamp), 0)
	binlog.DatetimeStart = utils.NewTime(startDatetime)
	binlog.BackupStartTime = utils.NewTime(time.Now())

	prefix := binlog.Storage.GetPrefix()
	dateDIR := startDatetime.Format("binlog-20060102")
	fileDIR := filepath.Join(prefix, dateDIR)
	startDatetimeStr := startDatetime.Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s%s", startDatetimeStr, name, fileEXT)
	binlog.FilePath = filepath.Join(fileDIR, filename)
	binlog.Storage.SetFilePath(binlog.FilePath)
	binlog.Status = "creating"
	return binlog, nil
}

func (bs *BinlogSet) SetMetaDB(MetaDB *gorm.DB) (err error) {
	bs.MetaDB = MetaDB
	return nil
}
