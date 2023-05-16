package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"mybackup/helper"
	"mybackup/object"
	"mybackup/utils"

	formatter "github.com/antonfisher/nested-logrus-formatter"
	"github.com/glebarez/sqlite"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"gorm.io/gorm"
)

// Conf the backup config structure
type Conf struct {
	LogLevel              string        `json:"log_level,omitempty"`
	BaseDIR               string        `json:"base_dir,omitempty"`
	BaseDIRExpireDuration time.Duration `json:"base_dir_expire_duration,omitempty"`
	MetaDBPath            string        `json:"meta_db_path,omitempty"`
	FilePrefix            string        `json:"file_prefix,omitempty"`
	LogColor              bool          `json:"log_color,omitempty"`
	LogFilePath           string        `json:"log_file_path,omitempty"`
	Action                string        `json:"action,omitempty"`
	Source                object.Object `json:"source,omitempty"`
	Target                object.Object `json:"target,omitempty"`
	Utils                 *utils.Utils  `json:"utils,omitempty"`
	ResultFile            string        `json:"result_file,omitempty"`
	TaskName              string        `json:"task_name,omitempty"`
	TaskPID               int           `json:"task_pid"`
	TaskTime              time.Time     `json:"-"`
	MetaDB                *gorm.DB      `json:"-"`
}

// user defined unmarshall method
func (c *Conf) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}

	// get action
	if _, ok := items["action"]; !ok {
		return fmt.Errorf("no action name was specified")
	}
	err = json.Unmarshal(items["action"], &c.Action)
	if err != nil {
		return err
	}

	// get task_name
	if _, ok := items["task_name"]; !ok {
		return fmt.Errorf("no task_name was specified")
	}
	err = json.Unmarshal(items["task_name"], &c.TaskName)
	if err != nil {
		return err
	}

	targetType := ""
	sourceType := ""

	for k, v := range items {
		switch k {
		case "log_level":
			err = json.Unmarshal(v, &c.LogLevel)
			if err != nil {
				return err
			}
		case "base_dir":
			err = json.Unmarshal(v, &c.BaseDIR)
			if err != nil {
				return err
			}
		case "base_dir_expire_duration":
			tmpStr := ""
			err = json.Unmarshal(v, &tmpStr)
			if err != nil {
				return err
			}
			c.BaseDIRExpireDuration, err = time.ParseDuration(tmpStr)
			if err != nil {
				return err
			}
			// c.BaseDIRExpireDuration at least one day
			if c.BaseDIRExpireDuration < 24*time.Hour {
				c.BaseDIRExpireDuration = 24 * time.Hour
			}
		case "file_prefix":
			err = json.Unmarshal(v, &c.FilePrefix)
			if err != nil {
				return err
			}
		case "meta_db_path":
			err = json.Unmarshal(v, &c.MetaDBPath)
			if err != nil {
				return err
			}
		case "action":
			// already parsed
			continue
		case "task_name":
			// already parsed
			continue
		case "source":
			typeHelper := helper.TypeHelper{}
			err = json.Unmarshal(v, &typeHelper)
			if err != nil {
				return fmt.Errorf("unmarshal type of source failed: %w", err)
			}
			sourceType = typeHelper.Type
			switch sourceType {
			case "mysql":
				c.Source = &object.MySQL{}
			case "postgresql":
				c.Source = &object.PostgreSQL{}
			case "snapshot":
				c.Source = &object.Snapshot{}
			case "snapshot_set":
				c.Source = &object.SnapshotSet{}
			case "file":
				c.Source = &object.File{}
			case "file_set":
				c.Source = &object.FileSet{}
			case "binlog_stream":
				c.Source = &object.BinlogStream{}
			case "binlog_set":
				c.Source = &object.BinlogSet{}
			case "pg_backup":
				c.Source = &object.PGBackup{}
			default:
				return fmt.Errorf("unknown source type: %s", sourceType)
			}
			err = json.Unmarshal(v, c.Source)
			if err != nil {
				return err
			}
		case "target":
			typeHelper := helper.TypeHelper{}
			err = json.Unmarshal(v, &typeHelper)
			if err != nil {
				return fmt.Errorf("unmarshal type of target failed: %w", err)
			}
			targetType = typeHelper.Type
			switch targetType {
			case "mysql":
				c.Target = &object.MySQL{}
			case "snapshot":
				c.Target = &object.Snapshot{}
			case "snapshot_set":
				c.Target = &object.SnapshotSet{}
			case "pg_backup":
				c.Target = &object.PGBackup{}
			case "file":
				c.Target = &object.File{}
			case "file_set":
				c.Target = &object.FileSet{}
			case "binlog_set":
				c.Target = &object.BinlogSet{}
			case "postgresql":
				c.Target = &object.PostgreSQL{}
			default:
				return fmt.Errorf("unknown target type: %s", targetType)
			}
			err = json.Unmarshal(v, c.Target)
			if err != nil {
				return err
			}
		case "utils":
			err = json.Unmarshal(v, &c.Utils)
			if err != nil {
				return err
			}
		case "result_file":
			err = json.Unmarshal(v, &c.ResultFile)
			if err != nil {
				return err
			}
		case "log_file_path":
			err = json.Unmarshal(v, &c.LogFilePath)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	// set default LogLevel
	if c.LogLevel == "" {
		c.LogLevel = "DEBUG"
	}
	// set TaskTime
	c.TaskTime = time.Now()
	// set TaskPID
	c.TaskPID = os.Getpid()
	// set FilePrefix
	if c.FilePrefix == "" {
		c.FilePrefix = fmt.Sprintf("%s-%s-%s-%s-%d", c.Action, c.TaskName, targetType, c.TaskTime.Format("20060102_150405"), c.TaskPID)
	}
	return err
}

func (c *Conf) SetMetaDB() (err error) {
	if c.MetaDBPath == "" {
		c.MetaDBPath = fmt.Sprintf("%s.db", filepath.Join(c.BaseDIR, c.TaskName))
	}
	c.MetaDB, err = gorm.Open(sqlite.Open(c.MetaDBPath), &gorm.Config{})
	if err != nil {
		return err
	}
	if c.Source != nil {
		err = c.Source.SetMetaDB(c.MetaDB)
		if err != nil {
			return err
		}
	}
	if c.Target != nil {
		err = c.Target.SetMetaDB(c.MetaDB)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Conf) SetLogFiles() (err error) {
	// set taskFileFullPath
	logFilePathPrefix := filepath.Join(c.BaseDIR, c.FilePrefix)

	if c.ResultFile == "" {
		c.ResultFile = fmt.Sprintf("%s.result", logFilePathPrefix)
	}
	// init log config
	log.SetReportCaller(true)
	log.SetFormatter(&formatter.Formatter{
		NoColors:        !c.LogColor,
		HideKeys:        true,
		TimestampFormat: time.RFC3339,
		CustomCallerFormatter: func(f *runtime.Frame) string {
			return fmt.Sprintf(" (%s:%d %s)", filepath.Base(f.File), f.Line, f.Function)
		},
	})
	// set log level
	level, err := log.ParseLevel(c.LogLevel)
	if err != nil {
		return err
	}
	log.SetLevel(level)
	// set log file if no log file is provided
	if c.LogFilePath == "" {
		c.LogFilePath = fmt.Sprintf("%s.log", logFilePathPrefix)
	}
	// log both to file and console stdout
	logWriter := io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   c.LogFilePath,
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     30,   // days
		Compress:   true, // disabled by default
	})
	log.SetOutput(logWriter)

	// config utils if exists
	if c.Utils == nil {
		return nil
	}
	if c.Utils.Xtrabackup != nil {
		c.Utils.Xtrabackup.LogWriter = logWriter
		c.Utils.Xtrabackup.FilePrefix = logFilePathPrefix
	}
	if c.Utils.Xbstream != nil {
		c.Utils.Xbstream.LogWriter = logWriter
	}
	if c.Utils.PgBackRest != nil {
		absBaseDIR, err := filepath.Abs(c.BaseDIR)
		if err != nil {
			return fmt.Errorf("get absolute path of %s failed: %w", c.BaseDIR, err)
		}
		if c.Utils.PgBackRest.LogPath == "" {
			c.Utils.PgBackRest.LogPath = filepath.Join(absBaseDIR, "log")
		}
		if c.Utils.PgBackRest.LockPath == "" {
			c.Utils.PgBackRest.LockPath = filepath.Join(absBaseDIR, "lock")
		}
		if c.Utils.PgBackRest.SpoolPath == "" {
			c.Utils.PgBackRest.SpoolPath = filepath.Join(absBaseDIR, "spool")
		}
		if c.Utils.PgBackRest.ConfigFile == "" {
			c.Utils.PgBackRest.ConfigFile = filepath.Join(absBaseDIR, "pgbackrest_"+c.Action+".conf")
		}
		if c.Utils.PgBackRest.Action == "" {
			c.Utils.PgBackRest.Action = c.Action
			if c.Action == "recover" {
				c.Utils.PgBackRest.Action = "restore"
			}
		}
	}
	return nil
}

// verify config
func (c *Conf) VerifyConf() (err error) {
	if c.Action != "list" && c.Source == nil {
		return fmt.Errorf("missing config 'source' field for action '%s'", c.Action)
	}
	if c.Target == nil {
		return fmt.Errorf("missing config 'target' for action '%s'", c.Action)
	}
	return nil
}

// ReadConfig read backup config file
func ReadConfig(configFile string) (*Conf, error) {
	// init config
	config := &Conf{}

	// read config file
	file, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	// parse config file
	err = json.Unmarshal([]byte(file), config)
	if err != nil {
		return nil, err
	}
	if config.Utils == nil {
		config.Utils = new(utils.Utils)
	}

	err = config.VerifyConf()

	return config, err
}
