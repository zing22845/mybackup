package object

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"mybackup/storage"
	"mybackup/utils"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"gorm.io/gorm"
)

// Snapshot MySQL physical backup result
type PGBackup struct {
	File
	DataPath               string     `json:"data_path,omitempty"`
	BackupPointTime        *time.Time `json:"backup_point_time,omitempty"`
	BackupType             string     `json:"backup_type,omitempty"`
	BackupDelta            bool       `json:"backup_delta,omitempty"`
	BackupFullIntervalType string     `json:"backup_full_interval_type,omitempty"`
	BackupFullInterval     uint16     `json:"backup_full_interval,omitempty"`
	RecoverType            string     `json:"recover_type,omitempty"`
	RecoverTarget          string     `json:"recover_target,omitempty"`
	RecoverDelta           bool       `json:"recover_delta,omitempty"`
	RetentionFullType      string     `json:"retention_full_type,omitempty"`
	RetentionFull          uint16     `json:"retention_full,omitempty"`
	RetentionHistory       uint16     `json:"retention_history,omitempty"`
	Bundle                 bool       `json:"bundle,omitempty"`
	RepoName               string     `json:"repo_name,omitempty"`
	MetaDB                 *gorm.DB   `json:"-"`
}

// user defined unmarshall method
// nolint:gocyclo
func (pgb *PGBackup) UnmarshalJSON(data []byte) (err error) {
	// set default values
	pgb.Bundle = true

	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	fileItems := make(map[string]json.RawMessage)

	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &pgb.Type)
			if err != nil {
				return err
			}
		case "data_path":
			err = json.Unmarshal(v, &pgb.DataPath)
			if err != nil {
				return err
			}
		case "storage", "pack_method",
			"expire_method", "expire_datetime", "expire_duration",
			"encrypt_method", "encrypt_key",
			"decrypt_method", "decrypt_key",
			"origin_size", "size",
			"origin_count", "count",
			"md5_sum", "base_dir", "speed_limit", "compress_method":
			fileItems[k] = v
		case "backup_type":
			err = json.Unmarshal(v, &pgb.BackupType)
			if err != nil {
				return err
			}
		case "recover_type":
			err = json.Unmarshal(v, &pgb.RecoverType)
			if err != nil {
				return err
			}
		case "recover_target":
			err = json.Unmarshal(v, &pgb.RecoverTarget)
			if err != nil {
				return err
			}
		case "recover_delta":
			err = json.Unmarshal(v, &pgb.RecoverDelta)
			if err != nil {
				return err
			}
		case "retention_full_type":
			err = json.Unmarshal(v, &pgb.RetentionFullType)
			if err != nil {
				return err
			}
		case "retention_full":
			err = json.Unmarshal(v, &pgb.RetentionFull)
			if err != nil {
				return err
			}
		case "retention_history":
			err = json.Unmarshal(v, &pgb.RetentionHistory)
			if err != nil {
				return err
			}
		case "bundle":
			err = json.Unmarshal(v, &pgb.Bundle)
			if err != nil {
				return err
			}
		case "backup_delta":
			err = json.Unmarshal(v, &pgb.BackupDelta)
			if err != nil {
				return err
			}
		case "backup_full_interval_type":
			err = json.Unmarshal(v, &pgb.BackupFullIntervalType)
			if err != nil {
				return err
			}
		case "backup_full_interval":
			err = json.Unmarshal(v, &pgb.BackupFullInterval)
			if err != nil {
				return err
			}
		case "repo_name":
			err = json.Unmarshal(v, &pgb.RepoName)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	fileItemsData, err := json.Marshal(fileItems)
	if err != nil {
		return err
	}
	err = json.Unmarshal(fileItemsData, &pgb.File)
	if err != nil {
		return err
	}
	return nil
}

// Download
func (pgb *PGBackup) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport operation now")
}

// Backup backup pg_backup to another Object
func (pgb *PGBackup) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport operation now")
}

// Recover recover from pg_backup to postgres
func (pgb *PGBackup) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	switch t := (targetObject).(type) {
	case *PostgreSQL:
		// calculate time
		recoverTimer := utils.NewTimer()
		t.Timers["recover"] = recoverTimer
		defer func() {
			recoverTimer.Stop()
			log.Infof("duration of total recovery: %s",
				recoverTimer.Duration.String(),
			)
		}()
		// set datapath
		pgb.DataPath = t.DataPath
		// prepare pgbackreset
		err = pgb.preparePgBackRest(u, "")
		if err != nil {
			return err
		}
		// generate config file
		err = u.PgBackRest.PrepareRun()
		if err != nil {
			return err
		}
		// recover
		err = u.PgBackRest.Restore(pgb.RepoName)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("does not support recover snapshot to %s", t.GetType())
	}
	return err
}

func (pgb *PGBackup) determinBackupType(u utils.Utils) (err error) {
	// all full backup
	if pgb.BackupType == "full" {
		return nil
	}
	u.PgBackRest.OutputFormat = "json"
	// parse info to determin full, incr or diff backup
	infoOut, err := u.PgBackRest.Info(pgb.RepoName)
	if err != nil {
		return err
	}
	labels := gjson.Get(infoOut, "0.backup.#.label")
	log.Debugf("labels before backup: %s", labels.String())
	if len(labels.Array()) == 0 {
		// first full backup
		u.PgBackRest.Type = "full"
		return nil
	}
	var lastFullIDX, lastIDX int
	for n, v := range labels.Array() {
		lastIDX = n
		if strings.HasSuffix(v.String(), "F") {
			lastFullIDX = n
		}
	}
	switch pgb.BackupFullIntervalType {
	case "time":
		lastFullLabel := labels.Array()[lastFullIDX].String()
		lastFullBackupDateStr := strings.Split(lastFullLabel, "-")[0]
		lastFullBackupDate, err := strconv.Atoi(lastFullBackupDateStr)
		if err != nil {
			return err
		}
		currentDate, _ := strconv.Atoi(time.Now().Format("20060102"))
		if currentDate-lastFullBackupDate > int(pgb.BackupFullInterval) {
			u.PgBackRest.Type = "full"
		}
	case "count":
		if lastIDX-lastFullIDX >= int(pgb.BackupFullInterval) {
			u.PgBackRest.Type = "full"
		}
	default:
		return fmt.Errorf("invalid BackupFullIntervalType")
	}
	return nil
}

// init pgBackReset members by pgBackup
func (pgb *PGBackup) preparePgBackRest(u utils.Utils, clusterRole string) error {
	// check pgbackrest
	if u.PgBackRest == nil {
		return fmt.Errorf("only support pgBackReset to backup PostgreSQL now")
	}
	// check Main
	if u.PgBackRest.Main == "" {
		return fmt.Errorf("no main file of pgBackRest found")
	}
	// backup
	switch u.PgBackRest.Action {
	case "backup":
		// set delta
		u.PgBackRest.Delta = pgb.BackupDelta
		// backup type
		u.PgBackRest.Type = pgb.BackupType
	case "restore":
		// set recover delta
		//   false: pg datadir must be empty
		//   true: allow no empty datadir
		u.PgBackRest.Delta = pgb.RecoverDelta
		// recover type
		u.PgBackRest.Type = pgb.RecoverType
		// recover target
		u.PgBackRest.Target = pgb.RecoverTarget
	}

	// stanza
	u.PgBackRest.StanzaList = make(map[string]*utils.Stanza, 0)
	stanza := &utils.Stanza{}
	u.PgBackRest.StanzaList[pgb.RepoName] = stanza
	err := pgb.prepareStanza(stanza, clusterRole)
	if err != nil {
		return err
	}
	return nil
}

func (pgb *PGBackup) prepareStanza(stanza *utils.Stanza, clusterRole string) (err error) {
	// stanza.PG1Path
	stanza.PG1Path = pgb.DataPath
	// stanza.RepoID
	stanza.RepoID = 1
	// stanza.BackupStandby
	if clusterRole == "standby" {
		stanza.BackupStandby = true
	}
	// compress method
	switch pgb.CompressMethod {
	case "gzip", "gz":
		stanza.CompressType = "gz"
	default:
		stanza.CompressType = pgb.CompressMethod
	}
	// bundle
	stanza.Bundle = pgb.Bundle
	// retention full type
	stanza.RetentionFullType = pgb.BackupFullIntervalType
	// retention full
	stanza.RetentionFull = pgb.RetentionFull
	// retention history
	stanza.RetentionHistory = pgb.RetentionHistory
	// stanza.RepoType
	switch t := pgb.Storage.(type) {
	case *storage.FS:
		stanza.RepoType = "posix"
		stanza.Path = t.Prefix
	case *storage.S3:
		stanza.RepoType = "s3"
		stanza.Path = t.Prefix
		stanza.S3Bucket = t.Bucket
		stanza.S3Endpoint = t.Endpoint
		stanza.S3Region = t.Region
		stanza.S3Key = t.AccessKey
		stanza.S3KeySecret = t.SecretKey
		stanza.S3Token = t.Token
		if t.ForcePathStyle {
			stanza.S3URIStyle = "path"
		} else {
			stanza.S3URIStyle = "host"
		}
		stanza.StorageVerifyTLS = t.SecureTLS
	default:
		return fmt.Errorf("unknown storage type")
	}
	return nil
}

func (pgb *PGBackup) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("unsupport operation now")
}

func (pgb *PGBackup) List(ctx context.Context, u utils.Utils) (err error) {
	// prepare pgbackreset
	err = pgb.preparePgBackRest(u, "")
	if err != nil {
		return err
	}
	// generate config file
	err = u.PgBackRest.PrepareRun()
	if err != nil {
		return err
	}
	// parse info to determin full, incr or diff backup
	infoOut, err := u.PgBackRest.Info(pgb.RepoName)
	if err != nil {
		return err
	}
	log.Infof("pgBackRest info of %s: %s", pgb.RepoName, infoOut)
	return nil
}

func (pgb *PGBackup) SetMetaDB(MetaDB *gorm.DB) (err error) {
	pgb.MetaDB = MetaDB
	return nil
}
