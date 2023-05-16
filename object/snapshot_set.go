package object

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"mybackup/storage"
	"mybackup/utils"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// set of MySQL backup snapshot
type SnapshotSet struct {
	Type                string                     `json:"type,omitempty"`
	SnapshotSet         []*Snapshot                `json:"snapshot_set,omitempty"`
	DefaultStorageID    string                     `json:"default_storage_id,omitempty"`
	Storages            map[string]storage.Storage `json:"storages,omitempty"`
	DefaultStorage      storage.Storage            `json:"default_storage,omitempty"`
	Timers              map[string]*utils.Timer    `json:"timers,omitempty"`
	Status              string                     `json:"status,omitempty"`
	Error               string                     `json:"error,omitempty"`
	RecoverMethod       string                     `json:"recover_method,omitempty"`
	InputStreamsWriters []io.Writer                `json:"-"`
	InputMultiWriter    io.Writer                  `json:"-"`
	Err                 error                      `json:"-"`
	MetaDB              *gorm.DB                   `json:"-"`
}

// user defined unmarshall method
func (ss *SnapshotSet) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}

	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &ss.Type)
			if err != nil {
				return err
			}
		case "snapshot_set":
			err = json.Unmarshal(v, &ss.SnapshotSet)
			if err != nil {
				return err
			}
		case "default_storage_id":
			err = json.Unmarshal(v, &ss.DefaultStorageID)
			if err != nil {
				return err
			}
		case "storages":
			err = json.Unmarshal(v, &ss.Storages)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	if ss.DefaultStorageID != "" {
		var ok bool
		ss.DefaultStorage, ok = ss.Storages[ss.DefaultStorageID]
		if !ok {
			return fmt.Errorf("no default storage(%s) in storages", ss.DefaultStorageID)
		}
	}
	for n, snapshot := range ss.SnapshotSet {
		if snapshot.Storage == nil {
			snapshot.StorageID = ss.DefaultStorageID
			if snapshot.StorageID == "" {
				return fmt.Errorf("neither storage nor storage id for snapshot[%d]", n)
			}
			snapshot.Storage = ss.Storages[ss.DefaultStorageID]
		}
	}
	ss.InputStreamsWriters = make([]io.Writer, 0)
	ss.Timers = make(map[string]*utils.Timer)
	return nil
}

func (ss *SnapshotSet) GetType() string {
	return ss.Type
}

// Move
func (ss *SnapshotSet) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

// Upload
func (ss *SnapshotSet) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

// Download
func (ss *SnapshotSet) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	// download each snapshot to target
	for n, snapshot := range ss.SnapshotSet {
		log.Infof("downloading snapshot %d", n)
		err = snapshot.Download(ctx, u, targetObject, overwrite)
		if err != nil {
			return err
		}
	}
	return err
}

// Backup backup Snapshot to another Object
func (ss *SnapshotSet) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport operation now")
}

// Recover recover from SnapshotSet to snapshot
func (ss *SnapshotSet) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	// check type
	targetSnapshot, ok := (targetObject).(*Snapshot)
	if !ok {
		return fmt.Errorf("only support snapshot as target now")
	}
	// calculate time
	targetSnapshot.Timers["recover"] = utils.NewTimer()
	defer func() {
		targetSnapshot.Timers["recover"].Stop()
		log.Infof("duration of total recovery: %s",
			targetSnapshot.Timers["recover"].Duration.String(),
		)
	}()
	err = ss.recoverSerial(ctx, u, targetSnapshot, overwrite)
	if err != nil {
		return err
	}
	return nil
}

func (ss *SnapshotSet) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("not support")
}

func (ss *SnapshotSet) GetStorageFilePath() string {
	log.Errorf("not support yet")
	return ""
}

func (ss *SnapshotSet) SetStorageFilePath(filePath string) {
	log.Errorf("not support yet")
}

func (ss *SnapshotSet) SetDefaultBaseDIR(baseDIR string) (err error) {
	for n, snapshot := range ss.SnapshotSet {
		log.Infof("snapshot[%d]:", n)
		err := snapshot.SetDefaultBaseDIR(baseDIR)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ss *SnapshotSet) SetDefaultFileName(filename string) (err error) {
	for n, snapshot := range ss.SnapshotSet {
		log.Infof("snapshot[%d]:", n)
		err := snapshot.SetDefaultFileName(filename)
		if err != nil {
			return err
		}
	}
	return nil
}

// List expired snapshot
func (ss *SnapshotSet) List(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("not support")
}

func (ss *SnapshotSet) SetMetaDB(MetaDB *gorm.DB) (err error) {
	ss.MetaDB = MetaDB
	for _, snapshot := range ss.SnapshotSet {
		snapshot.MetaDB = MetaDB
	}
	return nil
}

func (ss *SnapshotSet) WriteMeta() error {
	if ss.Err != nil {
		ss.Error = ss.Err.Error()
		ss.Status = "failed"
	}
	// rewrite metadata status = success_xxxx to status = success
	successCount := 0
	for _, snapshot := range ss.SnapshotSet {
		if strings.HasPrefix(snapshot.Status, "success") {
			snapshot.Status = "success"
			ss.Err = snapshot.WriteMeta()
			if ss.Err != nil {
				ss.Error = ss.Err.Error()
				ss.Status = "failed"
				return ss.Err
			}
			successCount++
		}
	}
	if successCount == len(ss.SnapshotSet) {
		ss.Status = "success"
	}
	return ss.Err
}

func (ss *SnapshotSet) sortSerial() {
	// sort snapshot set by FromLSN
	sort.Slice(ss.SnapshotSet, func(i, j int) bool {
		fromLSNI, err := strconv.Atoi(strings.ReplaceAll(ss.SnapshotSet[i].FromLSN, ":", ""))
		if err != nil {
			log.Warnf("invalid from lsn: %s, error: %s", ss.SnapshotSet[i].FromLSN, err.Error())
			return false
		}
		fromLSNJ, err := strconv.Atoi(strings.ReplaceAll(ss.SnapshotSet[j].FromLSN, ":", ""))
		if err != nil {
			log.Warnf("invalid from lsn: %s, error: %s", ss.SnapshotSet[j].FromLSN, err.Error())
			return true
		}
		return fromLSNI < fromLSNJ
	})
}

func (ss *SnapshotSet) recoverSerial(ctx context.Context, u utils.Utils, target *Snapshot, overwrite bool) (err error) {
	ss.sortSerial()
	// download snapshot
	for n, source := range ss.SnapshotSet {
		// download
		log.Infof("snapshot[%d]: downloading", n)
		err = source.Download(ctx, u, target, overwrite)
		if err != nil {
			return err
		}
		// set base dir and incremental dir for xtrabackup
		log.Infof(" snapshot[%d]: preparing", n)
		if target.BaseDIR == "" {
			log.Debugf("no base directory was specified")
		}
		// set --apply-log-only option for xtrabackup
		if n != len(ss.SnapshotSet)-1 {
			u.Xtrabackup.ApplyLogOnly = true
		} else {
			u.Xtrabackup.ApplyLogOnly = false
		}
		// prepare data
		err = target.Prepare(ctx, u)
		if err != nil {
			return err
		}
		target.Storage.SetFilePath("")
	}
	target.Storage.SetFilePath(target.BaseDIR)
	return ss.Err
}

func (ss *SnapshotSet) prepareParallel(ctx context.Context, u utils.Utils) error {
	var wg sync.WaitGroup
	for n, snapshot := range ss.SnapshotSet {
		log.Infof("snapshot[%d]: storage type %s, status %s",
			n, snapshot.Storage.GetType(), snapshot.Status)
		if strings.HasPrefix(snapshot.Status, "success") {
			_, ok := snapshot.Storage.(*storage.FS)
			if !ok {
				log.Warnf("snapshot[%d] skip prepare on storage %s", n, snapshot.Storage.GetType())
				continue
			}
			log.Infof("snapshot[%d]: preparing", n)
			wg.Add(1)
			go func(snapshot *Snapshot) {
				defer wg.Done()
				ss.Err = snapshot.Prepare(ctx, u)
			}(snapshot)
		}
	}
	wg.Wait()
	return ss.Err
}
