package object

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"sync"

	"fmt"
	"mybackup/model"
	"mybackup/storage"
	"mybackup/utils"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MySQL backup source
type MySQL struct {
	*model.MySQLClient
	*model.MySQLServer
	Type       string                  `json:"type,omitempty"`
	SpeedLimit string                  `json:"speed_limit,omitempty"`
	Timers     map[string]*utils.Timer `json:"timers,omitempty"`
	Storage    storage.Storage         `json:"storage,omitempty"`
	Databases  model.DBList            `json:"databases,omitempty"`
	Name       string                  `json:"-"`
	MetaDB     *gorm.DB                `json:"-"`
	Context    context.Context         `json:"-"`
}

func (m *MySQL) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	mysqlClientItems := make(map[string]json.RawMessage)
	mysqlServerItems := make(map[string]json.RawMessage)
	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &m.Type)
			if err != nil {
				return err
			}
		case "speed_limit":
			err = json.Unmarshal(v, &m.SpeedLimit)
			if err != nil {
				return err
			}
		case "user", "password", "socket", "defaults_file", "port", "host":
			mysqlClientItems[k] = v
		case "server_defaults_file", "mysqld_safe_path", "skip_check_pid_by_port":
			mysqlServerItems[k] = v
		case "databases":
			err = json.Unmarshal(v, &m.Databases)
			if err != nil {
				return err
			}
		case "exec":
			err = json.Unmarshal(v, &m.Exec)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	// init mysql client by config
	mysqlClientData, err := json.Marshal(mysqlClientItems)
	if err != nil {
		return err
	}
	err = json.Unmarshal(mysqlClientData, &m.MySQLClient)
	if err != nil {
		return err
	}
	// init mysql server by admin client
	m.MySQLServer, err = model.NewMySQLServer(m.MySQLClient)
	if err != nil {
		return err
	}
	// set mysql server variables by config
	mysqlServerData, err := json.Marshal(mysqlServerItems)
	if err != nil {
		return err
	}
	err = json.Unmarshal(mysqlServerData, &m.MySQLServer)
	if err != nil {
		return err
	}
	m.Context = context.Background()
	m.Timers = make(map[string]*utils.Timer)
	return nil
}

func (m *MySQL) GetType() string {
	return m.Type
}

// Upload
func (m *MySQL) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

// Move
func (m *MySQL) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

// prepare source mysql
func (m *MySQL) prepareSource() (err error) {
	// get source mysql server info
	err = m.MySQLServer.CollectInfo()
	if err != nil {
		return err
	}
	m.MySQLServer.Databases = m.Databases
	// set metadb by mysql server
	m.MySQLServer.Err = m.MetaDB.AutoMigrate(&model.MySQLServer{})
	if m.MySQLServer.Err != nil {
		return m.MySQLServer.Err
	}

	portStr, ok := m.GlobalVariables["port"]
	if !ok {
		return fmt.Errorf("get mysql server port failed")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	if !m.MySQLServer.SkipCheckPIDByPort {
		pid, err := utils.GetPidByListenPortAndCMD(port, "mysqld")
		if err != nil {
			return err
		}
		m.MySQLServer.PID = uint32(pid)
	}
	return m.WriteMeta()
}

func (m *MySQL) WriteMeta() error {
	if m.Err != nil {
		m.Status = "failed"
		m.Error = m.Err.Error()
	}
	m.MetaDB.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{
				{Name: "pid"},
				{Name: "version"},
				{Name: "server_defaults_file"},
				{Name: "databases"},
			},
			UpdateAll: true,
		}).Save(&m.MySQLServer)
	if m.MetaDB.Error != nil && m.Err == nil {
		m.Err = m.MetaDB.Error
		m.Error = m.Err.Error()
	}
	return m.Err
}

// Download
func (m *MySQL) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

func (m *MySQL) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	// prepare source mysql
	err = m.prepareSource()
	if err != nil {
		return err
	}
	// prepare xtrabackup
	err = m.prepareXtrabackup(u)
	if err != nil {
		return err
	}
	// backup
	switch t := targetObject.(type) {
	case *Snapshot:
		t.Err = m.physicalBackupToSingle(ctx, u, t, overwrite)
		if t.Err == nil {
			t.Status = "success"
		}
		return t.WriteMeta()
	case *SnapshotSet:
		t.Err = m.physicalBackupToMulti(ctx, u, t, overwrite)
		return t.WriteMeta()
	default:
		return fmt.Errorf("unsuport target type")
	}
}

func (m *MySQL) physicalBackupToMulti(ctx context.Context, u utils.Utils, target *SnapshotSet, overwrite bool) (err error) {
	if len(target.SnapshotSet) == 0 {
		return fmt.Errorf("empty target snapshot set")
	}
	var firstTarget *Snapshot
	for n, targetSnapshot := range target.SnapshotSet {
		if n == 0 {
			firstTarget = targetSnapshot
		}
		log.Infof("snapshot[%d] preparing single backup to %s",
			n, targetSnapshot.Storage.GetType())
		// set databases
		targetSnapshot.Databases = m.Databases
		// set MySQL version for snapshot
		targetSnapshot.MySQLVersion = m.Version
		// defer stop timer and write meta
		defer func(s *Snapshot) {
			s.Timers["backup"].Stop()
			log.Infof("snapshot[%d]: duration of backup: %s",
				n, s.Timers["backup"].Duration.String())
			s.BackupEndTime =
				s.Timers["backup"].StopTime
			if err != nil {
				s.Err = err
			} else {
				s.Status = "success_backup"
			}
			errMeta := s.WriteMeta()
			if errMeta != nil && err == nil {
				err = errMeta
			}
		}(targetSnapshot)
		// prepare download stream
		err = m.prepareSingleBackup(ctx, u, targetSnapshot, overwrite)
		if err != nil {
			return fmt.Errorf("snapshot[%d] prepare single backup to %s failed: %w",
				n, targetSnapshot.Storage.GetType(), err)
		}
		// add stream writers
		target.InputStreamsWriters = append(
			target.InputStreamsWriters,
			targetSnapshot.Stream.InputWriter)
	}
	// combile stream writers
	target.InputMultiWriter = io.MultiWriter(target.InputStreamsWriters...)
	// write xtrabackup stdout to combined multiwriter
	u.Xtrabackup.CMD.Stdout = target.InputMultiWriter
	// parse xtrabackup result from stderr
	logPipe, err := u.Xtrabackup.CMD.StderrPipe()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		err = firstTarget.ParseXtrabackupLogResult(logPipe, u.Xtrabackup.LogWriter)
		if err != nil {
			target.Err = err
		}
	}()
	// run cmd
	target.Err = u.Xtrabackup.CMD.Run()
	// wait ParseXtrabackupLogResult complete
	wg.Wait()
	var backupResult model.XtrabackupResult
	// done stream and check result
	for n, targetSnapshot := range target.SnapshotSet {
		if target.Err != nil {
			targetSnapshot.Err = target.Err
			continue
		}
		err = m.finishBackup(ctx, u, targetSnapshot)
		if err != nil {
			log.Infof("snapshot[%d]: post backup failed: %+v",
				n, err)
			targetSnapshot.Err = err
			continue
		}
		log.Infof("snapshot[%d]: backup success", n)
		// parse backup result from first snapshot and set backupResult for rest
		if n == 0 {
			targetSnapshot.Err = m.parseSnapshotTime(ctx, u, &targetSnapshot.XtrabackupResult)
			if targetSnapshot.Err != nil {
				target.Err = targetSnapshot.Err
				continue
			}
			backupResult = targetSnapshot.XtrabackupResult
		} else {
			targetSnapshot.XtrabackupResult = backupResult
		}
	}
	return target.Err
}

// delete failed backup file
func (m *MySQL) deleteFailedBackupFile(st storage.Storage) {
	targetFileInfo, warn := st.GetFileInfo()
	if warn != nil {
		log.Warnf("get status of file %s failed: %s", st.GetFilePath(), warn.Error())
		return
	}
	if targetFileInfo.FileStatus == "existent" {
		warn := st.DeleteFile()
		if warn != nil {
			log.Warnf(
				"delete failed target snapshot %s failed: %s",
				st.GetFilePath(), warn.Error(),
			)
		} else {
			log.Warnf(
				"delete failed target snapshot %s success",
				st.GetFilePath(),
			)
		}
	}
}

func (m *MySQL) finishBackup(ctx context.Context, u utils.Utils, target *Snapshot) error {
	// cancel wait stream finish
	target.Stream.Cancel()
	target.Stream.WaitGroup.Wait()
	// process err
	if target.Err == nil && target.Stream.Err != nil {
		target.Err = target.Stream.Err
	}
	// delete failed backup file
	if target.Err != nil && target.Storage.GetType() != "tcp" {
		m.deleteFailedBackupFile(target.Storage)
		return target.Err
	}
	// set size
	target.Size = target.Stream.OutputSize
	// get md5sum
	target.MD5Sum = target.Stream.OutputChecksum
	log.Infof("snapshot: the md5sum of streaming file is %s", target.MD5Sum)
	// deleting expired files
	if target.ExpireMethod != "" && target.Storage.GetType() != "tcp" {
		warn := target.DeleteFiles()
		if warn != nil {
			log.Warnf("delete expired file failed: %s", warn.Error())
		}
	}
	// remove empty TargetDIR
	warn := utils.RemoveEmptyDIR(u.Xtrabackup.TargetDIR)
	if warn != nil {
		log.Warnf("remove defaultTargetDIR failed: %s", warn)
	}

	// remove empty TMPDIR
	warn = utils.RemoveEmptyDIR(u.Xtrabackup.TMPDIR)
	if warn != nil {
		log.Warnf("remove defaultTMPDIR failed: %s", warn)
	}
	return target.Err
}

// Backup read from MySQL and write to writer
func (m *MySQL) physicalBackupToSingle(ctx context.Context, u utils.Utils, target *Snapshot, overwrite bool) (err error) {
	// defer stop timer and write meta
	defer func(s *Snapshot) {
		s.Timers["backup"].Stop()
		log.Infof("duration of backup: %s", s.Timers["backup"].Duration.String())
		s.BackupEndTime = s.Timers["backup"].StopTime
		if err != nil {
			s.Err = err
		} else {
			s.Status = "success_backup"
		}
		errMeta := s.WriteMeta()
		if errMeta != nil && err == nil {
			err = errMeta
		}
	}(target)
	// set dastabases
	target.Databases = m.Databases
	// set MySQL version
	target.MySQLVersion = m.Version
	// prepare backup
	err = m.prepareSingleBackup(ctx, u, target, overwrite)
	if err != nil {
		return fmt.Errorf("prepare single backup to %s failed: %w",
			target.Storage.GetType(), err)
	}
	// combine xtrabackup stdout to stream
	u.Xtrabackup.CMD.Stdout = target.Stream.InputWriter
	// parse xtrabackup result from stderr
	logPipe, err := u.Xtrabackup.CMD.StderrPipe()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		err = target.ParseXtrabackupLogResult(logPipe, u.Xtrabackup.LogWriter)
		if err != nil {
			target.Err = err
		}
	}()
	// run cmd
	target.Err = u.Xtrabackup.CMD.Run()
	// wait ParseXtrabackupLogResult complete
	wg.Wait()
	// finish backup, remove failed and expired backup file
	err = m.finishBackup(ctx, u, target)
	if err != nil {
		return err
	}
	// parseBackupResult
	target.Err = m.parseSnapshotTime(ctx, u, &target.XtrabackupResult)
	return target.Err
}

func (m *MySQL) prepareSingleBackup(ctx context.Context, u utils.Utils, target *Snapshot, overwrite bool) (err error) {
	// init timer
	target.Timers["backup"] = utils.NewTimer()
	// init metadb
	target.Err = target.MetaDB.AutoMigrate(&model.MySQLPhysicalBackup{})
	if target.Err != nil {
		return target.Err
	}
	// prepare metadata
	if target.DatetimeStart == nil {
		target.DatetimeStart = target.Timers["backup"].StartTime
	}
	target.BackupStartTime = target.Timers["backup"].StartTime
	target.Err = target.prepareMetaData("creating")
	if target.Err != nil {
		return target.Err
	}
	// set xtrabackup variable by target
	target.EncryptMethod = u.Xtrabackup.EncryptMethod
	target.EncryptKey = u.Xtrabackup.EncryptKey
	target.FromLSN = u.Xtrabackup.IncrementalLSN
	target.PackMethod = u.Xtrabackup.StreamMethod
	target.OriginSize = m.DataSize
	target.OriginCount = m.FileCount
	// set target FilePath
	targetFilePath := target.Storage.GetFilePath()
	if targetFilePath == "" {
		targetPrefix := target.Storage.GetPrefix()
		if targetPrefix == "" {
			target.Err = fmt.Errorf(
				"neither FilePath nor Prefix was specified",
			)
			return target.Err
		}
		if strings.HasSuffix("/", targetPrefix) {
			targetFilePath = filepath.Join(
				targetPrefix,
				fmt.Sprintf("%s-%d", target.DefaultFilename, target.ID),
			)
		} else {
			targetFilePath = fmt.Sprintf("%s%s-%d", targetPrefix, target.DefaultFilename, target.ID)
		}
		target.Storage.SetFilePath(targetFilePath)
	}
	target.Storage.SetFilePath(targetFilePath)
	log.Infof("to %s filepath: %s",
		target.Storage.GetType(),
		target.BaseDIR,
	)
	if m.Name == "" {
		m.Name = fmt.Sprintf("mysql_%d", m.Port)
	}
	// create target stream
	target.Stream = NewStream(ctx, 5*time.Second, m.Name)
	target.Stream.InputSize = m.DataSize
	target.Stream.OutputSize = m.DataSize
	target.Stream.InputSpeedLimit = m.SpeedLimit
	target.Stream.OutputSpeedLimit = target.SpeedLimit
	target.Stream.CompressMethod = target.CompressMethod
	target.Stream.DecompressMethod = target.DecompressMethod
	target.Stream.UnpackMethod = target.UnpackMethod
	// store stream
	target.Err = target.Stream.Store(u, target.Storage, target.DefaultFilename, overwrite)
	return target.Err
}

// init xtrabackup members by targetSnapshot
func (m *MySQL) prepareXtrabackup(u utils.Utils) (err error) {
	// check xtrabackup
	if u.Xtrabackup == nil {
		return fmt.Errorf("only support xtrabackup to backup MySQL now")
	}
	// set plugin_dir and keyring_file_data for TDE
	if m.GlobalVariables == nil {
		return fmt.Errorf("no MySQL server global variables found")
	}
	u.Xtrabackup.PluginDIR = m.GlobalVariables["plugin_dir"]
	u.Xtrabackup.KeyringFileData = m.GlobalVariables["keyring_file_data"]
	// set action backup
	u.Xtrabackup.Action = "backup"
	// set partial databases for xtrabackup
	u.Xtrabackup.Databases = m.Databases
	// set xtrabackup defaults file if empty
	u.Xtrabackup.CheckIsInnobackupex()
	if u.Xtrabackup.IsInnobackupex {
		u.Xtrabackup.DefaultsFile = m.MySQLServer.ServerDefaultsFile
	} else {
		u.Xtrabackup.DefaultsFile = m.MySQLClient.DefaultsFile
	}
	if m.User != "" {
		u.Xtrabackup.User = m.User
	}
	if m.Password != "" {
		u.Xtrabackup.Password = m.Password
	}
	if m.Socket != "" {
		u.Xtrabackup.Socket = m.Socket
	} else {
		u.Xtrabackup.Host = m.Host
		u.Xtrabackup.Port = m.Port
	}

	if u.Xtrabackup.Parallel <= 0 {
		u.Xtrabackup.Parallel = 4
	}
	if u.Xtrabackup.CompressThreads <= 0 {
		u.Xtrabackup.CompressThreads = 2
	}
	if u.Xtrabackup.Throttle <= 0 {
		u.Xtrabackup.Throttle = 100
	}
	// generate xtrabackup command
	err = u.Xtrabackup.GenerateCMD()
	if err != nil {
		return fmt.Errorf("generate xtrabackup command failed: %w", err)
	}
	m.Databases = u.Xtrabackup.Databases
	log.Infof("backup command: %s", u.Xtrabackup.MaskCMDStr)
	return nil
}

func (m *MySQL) parseSnapshotTime(ctx context.Context, u utils.Utils, xtrabackupResult *model.XtrabackupResult) (err error) {
	// check replica info
	err = m.MySQLServer.GetReplicaInfo(true)
	if err != nil {
		return err
	}
	log.Infof("==== start print replica status:")
	replicaStatus, err := json.MarshalIndent(m.MySQLServer.ReplicaStatus, "", "  ")
	if err != nil {
		return err
	}
	if xtrabackupResult.MasterIP != "" && m.MySQLServer.ReplicaStatus != nil {
		tmpMasterPort, err := strconv.Atoi(m.MySQLServer.ReplicaStatus["Master_Port"].(string))
		if err != nil {
			return err
		}
		xtrabackupResult.MasterPort = uint16(tmpMasterPort)
	}
	log.Infof(string(replicaStatus))
	log.Infof("==== end print replica status")
	// get snapshot time of backup server
	xtrabackupResult.BackupSnapshotTime, xtrabackupResult.BackupBinlogName, err =
		m.MySQLServer.GetEventBinlog(
			ctx,
			xtrabackupResult.BackupGTIDSet,
			xtrabackupResult.BackupBinlogName,
			xtrabackupResult.BackupBinlogPosition,
		)
	if err != nil {
		return err
	}
	xtrabackupResult.BinlogName = xtrabackupResult.BackupBinlogName
	xtrabackupResult.BinlogPosition = xtrabackupResult.BackupBinlogPosition
	log.Infof("backup snapshot time: %v", xtrabackupResult.BackupSnapshotTime.Time)
	if m.MySQLServer.ReplicaStatus != nil && m.MySQLServer.MasterServer != nil {
		// get snapshot time by event time
		xtrabackupResult.MasterSnapshotTime, xtrabackupResult.MasterBinlogName, err =
			m.MySQLServer.MasterServer.GetEventBinlog(
				ctx,
				xtrabackupResult.MasterGTIDSet,
				xtrabackupResult.MasterBinlogName,
				xtrabackupResult.MasterBinlogPosition,
			)
		if err != nil {
			return err
		}
		xtrabackupResult.BinlogName = xtrabackupResult.MasterBinlogName
		xtrabackupResult.BinlogPosition = xtrabackupResult.MasterBinlogPosition
		log.Infof("master snapshot time: %v", xtrabackupResult.MasterSnapshotTime.Time)
	}
	if xtrabackupResult.MasterSnapshotTime != nil &&
		xtrabackupResult.MasterSnapshotTime.Valid {
		xtrabackupResult.SnapshotTime = xtrabackupResult.MasterSnapshotTime
	} else {
		xtrabackupResult.SnapshotTime = xtrabackupResult.BackupSnapshotTime
	}
	log.Infof("snapshot time: %v", xtrabackupResult.SnapshotTime.Time)
	return err
}

// Recover read from reader and write to mysql
func (m *MySQL) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("not support yet")
}

func (m *MySQL) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("not support yet")
}

func (m *MySQL) GetStorageFilePath() string {
	log.Errorf("not support yet")
	return ""
}

func (m *MySQL) SetStorageFilePath(filePath string) {
	log.Errorf("not support yet")
}

func (m *MySQL) SetDefaultBaseDIR(baseDIR string) (err error) {
	return fmt.Errorf("not support yet")
}

func (m *MySQL) SetDefaultFileName(filename string) (err error) {
	return fmt.Errorf("not support yet")
}

func (m *MySQL) List(ctx context.Context, utils utils.Utils) (err error) {
	return fmt.Errorf("not support yet")
}

func (m *MySQL) SetMetaDB(MetaDB *gorm.DB) (err error) {
	m.MetaDB = MetaDB
	return nil
}
