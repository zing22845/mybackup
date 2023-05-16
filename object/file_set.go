package object

import (
	"context"
	"encoding/json"
	"fmt"

	"mybackup/storage"
	"mybackup/utils"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// set of files
type FileSet struct {
	Type             string                     `json:"type,omitempty"`
	FileSet          []*File                    `json:"file_set,omitempty"`
	DefaultStorageID string                     `json:"default_storage_id,omitempty"`
	Storages         map[string]storage.Storage `json:"storages,omitempty"`
	DefaultStorage   storage.Storage            `json:"default_storage,omitempty"`
	MetaDB           *gorm.DB                   `json:"-"`
}

// user defined unmarshall method
func (fst *FileSet) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}

	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &fst.Type)
			if err != nil {
				return err
			}
		case "file_set":
			err = json.Unmarshal(v, &fst.FileSet)
			if err != nil {
				return err
			}
		case "default_storage_id":
			err = json.Unmarshal(v, &fst.DefaultStorageID)
			if err != nil {
				return err
			}
		case "storages":
			err = json.Unmarshal(v, &fst.Storages)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}

	if fst.DefaultStorageID != "" {
		var ok bool
		fst.DefaultStorage, ok = fst.Storages[fst.DefaultStorageID]
		if !ok {
			return fmt.Errorf("no default storage(%s) in storages", fst.DefaultStorageID)
		}
	}

	for n, file := range fst.FileSet {
		if file.Storage == nil {
			file.StorageID = fst.DefaultStorageID
			if file.StorageID == "" {
				return fmt.Errorf("neither storage nor storage id for file[%d]", n)
			}
			file.Storage = fst.Storages[fst.DefaultStorageID]
		}
	}
	return nil
}

func (fst *FileSet) GetType() string {
	return fst.Type
}

// Move
func (fst *FileSet) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFileSet, ok := (targetObject).(*FileSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetFileSet.FileSet) != len(fst.FileSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetFileSet.FileSet), len(fst.FileSet))
	}

	// move each file to target
	for n, file := range fst.FileSet {
		log.Infof("moving file %d", n)
		err = file.Move(ctx, u, targetFileSet.FileSet[n], overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Upload
func (fst *FileSet) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFileSet, ok := (targetObject).(*FileSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetFileSet.FileSet) != len(fst.FileSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetFileSet.FileSet), len(fst.FileSet))
	}

	// upload each file to target
	for n, file := range fst.FileSet {
		log.Infof("uploading file %d", n)
		err = file.Upload(ctx, u, targetFileSet.FileSet[n], overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Download
func (fst *FileSet) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFileSet, ok := (targetObject).(*FileSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetFileSet.FileSet) != len(fst.FileSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetFileSet.FileSet), len(fst.FileSet))
	}

	// download each file to target
	for n, file := range fst.FileSet {
		log.Infof("downloading file %d", n)
		err = file.Download(ctx, u, targetFileSet.FileSet[n], overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Backup backup file set to another file set
func (fst *FileSet) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFileSet, ok := (targetObject).(*FileSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetFileSet.FileSet) != len(fst.FileSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetFileSet.FileSet), len(fst.FileSet))
	}

	// backup each file to target
	for n, file := range fst.FileSet {
		log.Infof("backuping file %d", n)
		err = file.Backup(ctx, u, targetFileSet.FileSet[n], overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Recover recover from file set to another file set
func (fst *FileSet) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetFileSet, ok := (targetObject).(*FileSet)
	if !ok {
		return fmt.Errorf("only support fileset as target now")
	}
	if len(targetFileSet.FileSet) != len(fst.FileSet) {
		return fmt.Errorf("target fileset count(%d) not match source fileset count(%d)", len(targetFileSet.FileSet), len(fst.FileSet))
	}

	// recover each file to target
	for n, file := range fst.FileSet {
		log.Infof("recovering file[%d]: %s", n, file.GetStorageFilePath())
		err = file.Recover(ctx, u, targetFileSet.FileSet[n], overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

func (fst *FileSet) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("not support")
}

func (fst *FileSet) GetStorageFilePath() string {
	log.Errorf("not support yet")
	return ""
}

func (fst *FileSet) SetStorageFilePath(filePath string) {
	log.Errorf("not support yet")
}

func (fst *FileSet) SetDefaultBaseDIR(baseDIR string) (err error) {
	for _, file := range fst.FileSet {
		err = file.SetDefaultBaseDIR(baseDIR)
		if err != nil {
			return nil
		}
	}
	return nil
}

func (fst *FileSet) SetDefaultFileName(filename string) (err error) {
	for _, file := range fst.FileSet {
		err = file.SetDefaultFileName(filename)
		if err != nil {
			return nil
		}
	}
	return nil
}

// List expired snapshot
func (fst *FileSet) List(ctx context.Context, utils utils.Utils) (err error) {
	return fmt.Errorf("not support")
}

func (fst *FileSet) SetMetaDB(MetaDB *gorm.DB) (err error) {
	fst.MetaDB = MetaDB
	return nil
}
