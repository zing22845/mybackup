package utils

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
)

// Xtrabackup xtrabackup struct, contain the xtrabackup command config etc.
type Xtrabackup struct {
	Main                           string              `json:"main,omitempty"`
	Parallel                       uint8               `json:"parallel,omitempty"`
	Ibbackup                       string              `json:"ibbackup,omitempty"`
	Throttle                       uint                `json:"throttle,omitempty"`
	CompressThreads                uint8               `json:"compress_threads,omitempty"`
	UseMemory                      string              `json:"use_memory,omitempty"`
	StreamMethod                   string              `json:"stream_method,omitempty"`
	DefaultsFile                   string              `json:"defaults_file,omitempty"`
	TMPDIR                         string              `json:"tmp_dir,omitempty"`
	TargetDIR                      string              `json:"target_dir,omitempty"`
	Action                         string              `json:"action,omitempty"`
	CompressSwitch                 uint8               `json:"compress_switch,omitempty"`
	EncryptMethod                  string              `json:"encrypt_method,omitempty"`
	EncryptKey                     string              `json:"encrypt_key,omitempty"`
	IncrementalLSN                 string              `json:"incremental_lsn,omitempty"`
	IncrementalDIR                 string              `json:"incremental_dir,omitempty"`
	PrepareDataDIR                 string              `json:"prepare_data_dir,omitempty"`
	PluginDIR                      string              `json:"plugin_dir,omitempty"`
	KeyringFileData                string              `json:"keyring_file_data,omitempty"`
	PrepareRemoveIncrementalDIR    bool                `json:"prepare_remove_incremental_dir,omitempty"`
	PrepareRemoveXtrabackupLogFile bool                `json:"prepare_remove_xtrabackup_log_file,omitempty"`
	Databases                      map[string][]string `json:"databases,omitempty"`
	DatabasesFile                  string              `json:"databases_file,omitempty"`
	Export                         bool                `json:"export,omitempty"`
	User                           string              `json:"-"`
	Password                       Password            `json:"-"`
	Host                           string              `json:"-"`
	Port                           uint16              `json:"-"`
	Socket                         string              `json:"-"`
	ApplyLogOnly                   bool                `json:"-"`
	MaskCMDStr                     string              `json:"-"`
	FilePrefix                     string              `json:"-"`
	IsInnobackupex                 bool                `json:"-"`
	CMD                            *exec.Cmd           `json:"-"`
	LogWriter                      io.Writer           `json:"-"`
}

// GenerateCMD generate xtrabackup command from args
func (x *Xtrabackup) GenerateCMD() (err error) {
	if x.Action == "backup" {
		return x.GenerateBackupCMD()
	} else if x.Action == "prepare" {
		return x.GeneratePrepareCMD()
	}
	return fmt.Errorf("unsupport xtrabackup action: %s", x.Action)
}

// check if main is innobackupex
func (x *Xtrabackup) CheckIsInnobackupex() {
	x.IsInnobackupex = strings.Contains(x.Main, "innobackupex")
}

// GenerateBackupCMD for backup
func (x *Xtrabackup) GenerateBackupCMD() (err error) {
	// check file prefix
	if x.FilePrefix == "" {
		return fmt.Errorf("file prefix is empty")
	}
	// check if innobackupex
	x.CheckIsInnobackupex()
	// set --parallel for xtrabackup
	xtrabackupOptParallel := ""
	if x.Parallel > 0 {
		xtrabackupOptParallel = fmt.Sprintf("--parallel=%d", x.Parallel)
	}
	// init xtrabackup options
	xtrabackupOptAction := ""
	xtrabackupOptDatabasesFile := ""
	xtrabackupOptPluginDIR := ""
	xtrabackupOptKeyringFileData := ""
	xtrabackupOptTarget := ""
	xtrabackupOptIbbackup := ""
	// set --target-dir
	if x.TargetDIR == "" {
		x.TargetDIR = fmt.Sprintf(
			"%s_target",
			x.FilePrefix,
		)
	}
	// set diff options between innobackupex and xtrabackup
	if x.IsInnobackupex {
		// set ibbbackup
		if x.Ibbackup != "" {
			xtrabackupOptIbbackup = fmt.Sprintf("--ibbackup=%s", x.Ibbackup)
		}
		// set trget dir
		xtrabackupOptTarget = x.TargetDIR
	} else {
		// set --databases-file
		if len(x.Databases) != 0 {
			err = x.createDatabasesFile()
			if err != nil {
				return err
			}
			xtrabackupOptDatabasesFile = fmt.Sprintf("--databases-file=%s", x.DatabasesFile)
		}
		// set action
		xtrabackupOptAction = "--backup"
		// set --xtrabackup-plugin-dir
		if x.PluginDIR != "" {
			xtrabackupOptPluginDIR = fmt.Sprintf(
				"--xtrabackup-plugin-dir=%s",
				x.PluginDIR,
			)
		}
		// set --keyring-file-data for TDE
		if x.KeyringFileData != "" {
			xtrabackupOptKeyringFileData = fmt.Sprintf(
				"--keyring-file-data=%s",
				x.KeyringFileData,
			)
		}
		// set target dir for xtrabackup
		xtrabackupOptTarget = fmt.Sprintf("--target-dir=%s", x.TargetDIR)
	}
	// create target dir
	err = CreateDIR(x.TargetDIR)
	if err != nil {
		return fmt.Errorf(
			"create target dir %s failed: %w",
			x.TargetDIR, err)
	}
	// set --tmpdir
	if x.TMPDIR == "" {
		x.TMPDIR = fmt.Sprintf(
			"%s_tmp",
			x.FilePrefix,
		)
	}
	x.TMPDIR, err = filepath.Abs(x.TMPDIR)
	if err != nil {
		return err
	}
	xtrabackupOptTMPDIR := fmt.Sprintf("--tmpdir=%s", x.TMPDIR)
	err = CreateDIR(x.TMPDIR)
	if err != nil {
		return fmt.Errorf("create temp dir %s failed: %w", x.TMPDIR, err)
	}
	// set --slave-info
	xtrabackupOptSlaveInfo := "--slave-info"
	// set --incremental --incremental-lsn
	xtrabackupOptIncremental := ""
	if x.IncrementalLSN != "0" && x.IncrementalLSN != "" {
		xtrabackupOptIncremental = fmt.Sprintf(
			"--incremental --incremental-lsn=%s",
			x.IncrementalLSN,
		)
	}
	// set mysql connection options
	if x.DefaultsFile == "" && x.User == "" {
		return fmt.Errorf("no MySQL connection info was found")
	}
	xtrabackupOptConnMySQL := ""
	xtrabackupOptConnMySQLMaskPassword := ""
	if x.DefaultsFile != "" {
		if _, err := os.Stat(x.DefaultsFile); os.IsNotExist(err) {
			return fmt.Errorf("MySQL defaults file %s was not found", x.DefaultsFile)
		}
		xtrabackupOptConnMySQL = fmt.Sprintf(
			"--defaults-file=%s",
			x.DefaultsFile,
		)
	}
	if x.User != "" {
		xtrabackupOptConnMySQL = xtrabackupOptConnMySQL + fmt.Sprintf(" --user='%s'", x.User)
	}
	if x.Socket != "" {
		xtrabackupOptConnMySQL = xtrabackupOptConnMySQL + fmt.Sprintf(" --socket='%s'", x.Socket)
		log.Infof("using socket")
	} else {
		xtrabackupOptConnMySQL = xtrabackupOptConnMySQL + fmt.Sprintf(" --host='%s'", x.Host)
		xtrabackupOptConnMySQL = xtrabackupOptConnMySQL + fmt.Sprintf(" --port='%d'", x.Port)
		log.Infof("using host and port: %s", xtrabackupOptConnMySQL)
	}
	if x.Password != "" {
		xtrabackupOptConnMySQLMaskPassword = xtrabackupOptConnMySQL + " --password='******'"
		xtrabackupOptConnMySQL = xtrabackupOptConnMySQL + fmt.Sprintf(" --password='%s'", x.Password)
	}
	// set --compress and --compress-threads
	xtrabackupOptCompress := ""
	if x.CompressSwitch == 1 {
		if x.CompressThreads > 1 {
			xtrabackupOptCompress = fmt.Sprintf(
				"--compress --compress-threads=%d",
				x.CompressThreads,
			)
		} else {
			xtrabackupOptCompress = "--compress"
		}
	}
	// set --encrypt and encrypt-key
	xtrabackupOptEncrypt := ""
	xtrabackupOptEncryptMaskEncryptKey := ""
	if x.EncryptMethod == "" {
		xtrabackupOptEncrypt = ""
	} else if !Contains(SupportEncryptMethod, x.EncryptMethod) {
		return fmt.Errorf(
			"unsupport encrypt method: %s",
			x.EncryptMethod,
		)
	} else if len(x.EncryptKey) != 32 {
		return fmt.Errorf(
			"invalid encrypt key length: %d",
			len(x.EncryptKey),
		)
	} else {
		xtrabackupOptEncrypt = fmt.Sprintf(
			"--encrypt=%s --encrypt-key=%s",
			x.EncryptMethod, x.EncryptKey)
		xtrabackupOptEncryptMaskEncryptKey = fmt.Sprintf(
			"--encrypt=%s --encrypt-key=******",
			x.EncryptMethod)
	}
	// set --stream
	xtrabackupOptStream := ""
	if x.StreamMethod == "xbstream" {
		xtrabackupOptStream = fmt.Sprintf("--stream=%s", x.StreamMethod)
	} else if x.StreamMethod == "" {
		xtrabackupOptStream = x.StreamMethod
	} else {
		return fmt.Errorf("invalid xtrabackup stream option")
	}

	// get xtrabackup command
	cmdStr := fmt.Sprintf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s",
		x.Main,
		xtrabackupOptConnMySQL,
		xtrabackupOptIbbackup,
		xtrabackupOptSlaveInfo,
		xtrabackupOptIncremental,
		xtrabackupOptParallel,
		xtrabackupOptCompress,
		xtrabackupOptEncrypt,
		xtrabackupOptPluginDIR,
		xtrabackupOptKeyringFileData,
		xtrabackupOptStream,
		xtrabackupOptAction,
		xtrabackupOptDatabasesFile,
		xtrabackupOptTMPDIR,
		xtrabackupOptTarget,
	)
	// get xtrabackup masked command
	x.MaskCMDStr = fmt.Sprintf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s",
		x.Main,
		xtrabackupOptConnMySQLMaskPassword,
		xtrabackupOptIbbackup,
		xtrabackupOptSlaveInfo,
		xtrabackupOptIncremental,
		xtrabackupOptParallel,
		xtrabackupOptCompress,
		xtrabackupOptEncryptMaskEncryptKey,
		xtrabackupOptPluginDIR,
		xtrabackupOptKeyringFileData,
		xtrabackupOptStream,
		xtrabackupOptAction,
		xtrabackupOptDatabasesFile,
		xtrabackupOptTMPDIR,
		xtrabackupOptTarget,
	)
	x.CMD = exec.Command("sh", "-c", cmdStr)
	return nil
}

// GenerateBackupCMD for backup
func (x *Xtrabackup) GeneratePrepareCMD() (err error) {
	// check if innobackupex
	x.CheckIsInnobackupex()
	// set --use-memory
	xtrabackupOptUseMemory := ""
	if x.UseMemory != "" {
		xtrabackupOptUseMemory = fmt.Sprintf("--use-memory=%s", x.UseMemory)
	}
	// check data dir exists
	if x.PrepareDataDIR == "" {
		return fmt.Errorf("prepare data dir is empty")
	}
	// check target dir
	if x.TargetDIR == "" {
		return fmt.Errorf(
			"no backup target dir, invalid backup command",
		)
	}
	prepareDataDIRStat, err := os.Stat(x.PrepareDataDIR)
	if err != nil {
		return fmt.Errorf("get data dir status failed: %w", err)
	}
	if !prepareDataDIRStat.IsDir() {
		return fmt.Errorf("prepare data dir(%s) is not a directory", x.PrepareDataDIR)
	}
	// read xtrabackup_checkpoints file
	xtrabackupCheckpointsFilePath := filepath.Join(x.PrepareDataDIR, "xtrabackup_checkpoints")
	xtrabackupCheckpoints, err := ini.Load(xtrabackupCheckpointsFilePath)
	if err != nil {
		return err
	}
	// get from LSN from xtrabackup_checkpints file
	fromLSN := xtrabackupCheckpoints.Section("").Key("from_lsn").String()
	// set TargetDIR and IncrementalDIR for xtrabackup prepare
	if fromLSN == "0" || fromLSN == "0:0" {
		absTargetDIR, err := filepath.Abs(x.TargetDIR)
		if err != nil {
			return fmt.Errorf("get abs path of TargetDIR(%s) failed: %w", x.TargetDIR, err)
		}
		absPrepareDataDIR, err := filepath.Abs(x.PrepareDataDIR)
		if err != nil {
			return fmt.Errorf("get abs path of PrepareDataDIR(%s) failed: %w", x.PrepareDataDIR, err)
		}
		if absTargetDIR != absPrepareDataDIR {
			// rename PrepareDataDIR to TargetDIR
			err = os.Rename(x.PrepareDataDIR, x.TargetDIR)
			if err != nil {
				return fmt.Errorf(
					"rename PrepareDataDIR(%s) to TargetDIR(%s) failed: %w",
					x.PrepareDataDIR, x.TargetDIR, err)
			}
		}
		log.Infof("rename PrepareDataDIR(%s) to TargetDIR(%s) success", x.PrepareDataDIR, x.TargetDIR)
		x.IncrementalDIR = ""
	} else {
		x.IncrementalDIR = x.PrepareDataDIR
	}
	// set --incremental-dir
	xtrabackupOptIncrementalDIR := ""
	if x.IncrementalDIR != "" {
		xtrabackupOptIncrementalDIR = fmt.Sprintf(
			"--incremental-dir=%s",
			x.IncrementalDIR,
		)
	}
	// set diff options between innobackupex and xtrabackup
	xtrabackupOptDefaultsFile := ""
	xtrabackupOptAction := ""
	xtrabackupOptApplyLogOnly := ""
	xtrabackupOptTarget := ""
	xtrabackupOptIbbackup := ""
	xtrabackupOptKeyringFileData := ""
	xtrabackupOptPluginDIR := ""
	xtrabackupOptExport := ""
	if x.IsInnobackupex {
		// set defaults file
		defaultsFilePath := filepath.Join(x.TargetDIR, "backup-my.cnf")
		xtrabackupOptDefaultsFile = fmt.Sprintf("--defaults-file=\"%s\"", defaultsFilePath)
		// set ibbbackup
		if x.Ibbackup != "" {
			xtrabackupOptIbbackup = fmt.Sprintf("--ibbackup=%s", x.Ibbackup)
		}
		if x.ApplyLogOnly {
			xtrabackupOptApplyLogOnly = "--apply-log --redo-only"
		} else {
			xtrabackupOptApplyLogOnly = "--apply-log"
		}
		// set trget dir
		xtrabackupOptTarget = x.TargetDIR
	} else {
		// set --prepare
		xtrabackupOptAction = "--prepare"
		// set --apply-log-only
		if x.ApplyLogOnly {
			xtrabackupOptApplyLogOnly = "--apply-log-only"
		}
		// set --export
		if x.Export {
			xtrabackupOptExport = "--export"
		}
		// set --xtrabackup-plugin-dir
		if x.PluginDIR != "" {
			xtrabackupOptPluginDIR = fmt.Sprintf(
				"--xtrabackup-plugin-dir=%s",
				x.PluginDIR,
			)
		}
		// set --keyring-file-data for TDE
		if x.KeyringFileData != "" {
			xtrabackupOptKeyringFileData = fmt.Sprintf(
				"--keyring-file-data=%s",
				x.KeyringFileData,
			)
		}
		// set --target-dir
		xtrabackupOptTarget = fmt.Sprintf("--target-dir=%s", x.TargetDIR)
	}
	// get xtrabackup command
	cmdStr := fmt.Sprintf("%s %s %s %s %s %s %s %s %s %s %s",
		x.Main,
		xtrabackupOptDefaultsFile,
		xtrabackupOptIbbackup,
		xtrabackupOptAction,
		xtrabackupOptExport,
		xtrabackupOptApplyLogOnly,
		xtrabackupOptUseMemory,
		xtrabackupOptPluginDIR,
		xtrabackupOptKeyringFileData,
		xtrabackupOptTarget,
		xtrabackupOptIncrementalDIR,
	)
	x.CMD = exec.Command("sh", "-c", cmdStr)
	x.CMD.Stderr = x.LogWriter
	return nil
}

func (x *Xtrabackup) createDatabasesFile() (err error) {
	/*
		mandatory databases for partial recovery:
		{
			"mysql": [],
			"sys": [],
			"performance_schema": [],
		}
	*/
	mandatoryDBs := []string{"mysql", "sys", "performance_schema"}
	log.Infof("mandatory databases for partial recovery: %s", mandatoryDBs)
	for _, db := range mandatoryDBs {
		if tables, ok := x.Databases[db]; !ok || len(tables) != 0 {
			x.Databases[db] = []string{}
		}
	}
	// get databases file path, must be abs path
	x.DatabasesFile = fmt.Sprintf("%s.dblist", x.FilePrefix)
	x.DatabasesFile, err = filepath.Abs(x.DatabasesFile)
	if err != nil {
		return err
	}
	if x.DatabasesFile == "" {
		return fmt.Errorf("can not set databases_file for xtrabackup")
	}
	// create databases-file
	dbListFile, err := os.Create(x.DatabasesFile)
	if err != nil {
		return fmt.Errorf("create databases-file for xtrabackup failed: %w", err)
	}
	defer dbListFile.Close()
	// write databases-file
	for db, tables := range x.Databases {
		if len(tables) == 0 {
			_, err = fmt.Fprintf(dbListFile, "%s\n", db)
			if err != nil {
				return err
			}
			continue
		}
		for _, tb := range tables {
			_, err = fmt.Fprintf(dbListFile, "%s.%s\n", db, tb)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
