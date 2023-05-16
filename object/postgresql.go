package object

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"mybackup/utils"
)

// MySQL backup source
type PostgreSQL struct {
	Type        string                  `json:"type,omitempty"`
	BaseDIR     string                  `json:"base_dir,omitempty"`
	DataPath    string                  `json:"data_path,omitempty"`
	ClusterRole string                  `json:"cluster_role,omitempty"`
	Timers      map[string]*utils.Timer `json:"timers,omitempty"`
	MetaDB      *gorm.DB                `json:"-"`
}

func (pg *PostgreSQL) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &pg.Type)
			if err != nil {
				return err
			}
		case "base_dir":
			err = json.Unmarshal(v, &pg.BaseDIR)
			if err != nil {
				return err
			}
		case "data_path":
			err = json.Unmarshal(v, &pg.DataPath)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	pg.Timers = make(map[string]*utils.Timer)
	return nil
}

func (pg *PostgreSQL) GetType() string {
	return pg.Type
}

// Upload
func (pg *PostgreSQL) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

// Move
func (pg *PostgreSQL) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

// Download
func (pg *PostgreSQL) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

func (pg *PostgreSQL) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	switch target := targetObject.(type) {
	case *PGBackup:
		return pg.physicalBackup(ctx, u, target, overwrite)
	default:
		return fmt.Errorf("unsuport target type")
	}
}

// Backup read from MySQL and write to writer
func (pg *PostgreSQL) physicalBackup(ctx context.Context, u utils.Utils, pgBackup *PGBackup, overwrite bool) (err error) {
	// calculate time
	backupTimer := utils.NewTimer()
	pgBackup.Timers["backup"] = backupTimer
	defer func() {
		backupTimer.Stop()
		log.Infof(
			"duration of backup: %s",
			backupTimer.Duration.String(),
		)
	}()
	// run pg_controldata to get pg cluster role
	err = pg.getPGClusterRole()
	if err != nil {
		return err
	}
	if pg.ClusterRole == "shut down" {
		return fmt.Errorf("pg '%s' was shut down", pg.DataPath)
	}
	pgBackup.DataPath = pg.DataPath
	// prepare pgbackreset
	err = pgBackup.preparePgBackRest(u, pg.ClusterRole)
	if err != nil {
		return err
	}
	// generate config file etc.
	err = u.PgBackRest.PrepareRun()
	if err != nil {
		return err
	}
	// create stanza
	err = u.PgBackRest.StanzaCreate(pgBackup.RepoName)
	if err != nil {
		return err
	}
	// check stanza
	err = u.PgBackRest.Check(pgBackup.RepoName)
	if err != nil {
		return err
	}
	// check backup type (check if modify u.PgBackRest.BackupType to 'full' by BackupFullIntervalType and BackupFullInterval)
	err = pgBackup.determinBackupType(u)
	if err != nil {
		return err
	}
	// backup
	err = u.PgBackRest.Backup(pgBackup.RepoName)
	if err != nil {
		return err
	}
	return nil
}

func (pg *PostgreSQL) getPGClusterRole() error {
	cmd := exec.Command("sh", "-c",
		fmt.Sprintf("%s/bin/pg_controldata %s", pg.BaseDIR, pg.DataPath))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("get cluster role of PG failed, cmd: %s, stderr: %s, err: %w", cmd, stderr.String(), err)
	}
	regClusterState := regexp.MustCompile(`^Database cluster state:\s*(.*)$`)
	for _, line := range strings.Split(stdout.String(), "\n") {
		subMatch := regClusterState.FindStringSubmatch(line)
		if len(subMatch) > 0 {
			switch subMatch[1] {
			case "in production":
				pg.ClusterRole = "primary"
			case "in archive recovery":
				pg.ClusterRole = "standby"
			case "shut down":
				pg.ClusterRole = "shut down"
			default:
				return fmt.Errorf("unsupport PG status: %s", subMatch[1])
			}
		}
	}
	if pg.ClusterRole == "" {
		return fmt.Errorf("get PG cluster role failed: %s", cmd.String())
	}
	return nil
}

// Recover read from reader and write to mysql
func (pg *PostgreSQL) Recover(ctx context.Context, u utils.Utils, targetSnapshot Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

func (pg *PostgreSQL) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("not support yet")
}

func (pg *PostgreSQL) GetStorageFilePath() string {
	log.Errorf("not support yet")
	return ""
}

func (pg *PostgreSQL) SetStorageFilePath(filePath string) {
	log.Errorf("not support yet")
}

func (pg *PostgreSQL) SetDefaultBaseDIR(baseDIR string) (err error) {
	return nil
}

func (pg *PostgreSQL) SetDefaultFileName(filename string) (err error) {
	return nil
}

func (pg *PostgreSQL) List(ctx context.Context, utils utils.Utils) (err error) {
	return fmt.Errorf("not support yet")
}

func (pg *PostgreSQL) SetMetaDB(MetaDB *gorm.DB) (err error) {
	pg.MetaDB = MetaDB
	return nil
}
