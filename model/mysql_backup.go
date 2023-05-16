package model

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"mybackup/utils"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
)

type MySQLDDLBackup struct {
	Backup
	EventDatetime    *utils.SQLNullTime `gorm:"uniqueIndex:uk_execdt_gtid" json:"event_datetime,omitempty"`
	ExecuteTime      uint32             `json:"execute_time,omitempty"`
	GTID             string             `gorm:"column:gtid;uniqueIndex:uk_execdt_gtid" json:"gtid,omitempty"`
	BinlogName       string             `json:"binlog_name,omitempty"`
	StartLogPosition uint32             `json:"start_log_position,omitempty"`
	EndLogPosition   uint32             `json:"end_log_position,omitempty"`
	Schema           string             `json:"schema,omitempty"`
	Query            string             `json:"query,omitempty"`
}

func NewMySQLDDLBackup(ExecuteTimestamp uint32, GTID, schema, query string) *MySQLDDLBackup {
	return &MySQLDDLBackup{
		EventDatetime: utils.NewTime(time.Unix(int64(ExecuteTimestamp), 0)),
		GTID:          GTID,
		Schema:        schema,
		Query:         query,
	}
}

func (MySQLDDLBackup) TableName() string {
	return "mysql_ddl_backup"
}

type XtrabackupResult struct {
	MasterIP             string             `json:"master_ip,omitempty"`
	MasterPort           uint16             `json:"master_port,omitempty"`
	FromLSN              string             `json:"from_lsn,omitempty"`
	ToLSN                string             `json:"to_lsn,omitempty"`
	BinlogName           string             `json:"binlog_name,omitempty"`
	BinlogPosition       uint32             `json:"binlog_position,omitempty"`
	SnapshotTime         *utils.SQLNullTime `gorm:"index" json:"snapshot_time,omitempty"`
	MasterGTIDSet        string             `gorm:"column:master_gtid_set" json:"master_gtid_set,omitempty"`
	MasterBinlogName     string             `json:"master_binlog_name,omitempty"`
	MasterBinlogPosition uint32             `json:"master_binlog_position,omitempty"`
	MasterSnapshotTime   *utils.SQLNullTime `json:"master_snapshot_time,omitempty"`
	BackupGTIDSet        string             `gorm:"column:backup_gtid_set" json:"backup_gtid_set,omitempty"`
	BackupBinlogName     string             `json:"backup_binlog_name,omitempty"`
	BackupBinlogPosition uint32             `json:"backup_binlog_position,omitempty"`
	BackupSnapshotTime   *utils.SQLNullTime `json:"backup_snapshot_time,omitempty"`
}

type TargetLine struct {
	REG      *regexp.Regexp
	SubMatch []string
}

func (l *TargetLine) ToJSONString() (jsonString string, err error) {
	jsonBytes, err := json.MarshalIndent(l.SubMatch, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonBytes), err
}

// parse xtrabackup log result
// nolint:gocyclo
func (r *XtrabackupResult) ParseXtrabackupLogResult(logReader io.Reader, logWriter io.Writer) (err error) {
	var (
		jsonString string
	)
	logLineComplete := &TargetLine{REG: regexp.MustCompile(`^[^"]*?completed OK!$`)}
	logLineToLSN := &TargetLine{REG: regexp.MustCompile(`^[^"]*?The latest check point \(for incremental\): +'?([0-9]+)'?`)}
	regGTIDComplete := regexp.MustCompile(`(.*?)'`)
	logLineMySQLPos := &TargetLine{
		REG: regexp.MustCompile(
			`^[^"]*?MySQL binlog position: filename '(.*?)', position '?([0-9]+)'` +
				`?(?:, (GTID) of the last change '(.*?)$)?`,
		)}
	isLogLineMySQLPosComplete := true
	logGTIDFlag := "GTID"
	logLineReplicaPos := &TargetLine{REG: regexp.MustCompile(
		`^[^"]*?MySQL slave binlog position: master host '(.*?)'?,(?: filename '(.*?)', position '?([0-9]+)'?)` +
			`?(?: (purge list) '(.*?)$)?`,
	)}
	isLogLineReplicaPosComplete := true
	logReplicaGTIDFlag := "purge list"
	// read from logReader and write to logWriter
	resultReader := io.TeeReader(logReader, logWriter)
	resultScanner := bufio.NewScanner(resultReader)
	// parse lines
	for resultScanner.Scan() {
		line := resultScanner.Text()
		if logLineToLSN.REG.MatchString(line) {
			logLineToLSN.SubMatch = logLineToLSN.REG.FindStringSubmatch(line)
			// debug to LSN line
			jsonString, err = logLineToLSN.ToJSONString()
			if err != nil {
				return err
			}
			log.Infof("log line to LSN:\n%s", jsonString)
			r.ToLSN = logLineToLSN.SubMatch[1]
			continue
		} else if logLineMySQLPos.REG.MatchString(line) || !isLogLineMySQLPosComplete {
			if isLogLineMySQLPosComplete {
				logLineMySQLPos.SubMatch = logLineMySQLPos.REG.FindStringSubmatch(line)
				if logLineMySQLPos.SubMatch[4] != "" {
					gtidSet := logLineMySQLPos.SubMatch[4]
					if regGTIDComplete.MatchString(gtidSet) {
						logLineMySQLPos.SubMatch[4] = regGTIDComplete.FindStringSubmatch(gtidSet)[1]
					} else {
						log.Infof("MySQL GTID is not completed: %s", logLineMySQLPos.SubMatch[4])
						isLogLineMySQLPosComplete = false
						continue
					}
				}
			} else {
				tmpStr := strings.TrimRight(line, ",")
				tmpComplete := regGTIDComplete.MatchString(line)
				if tmpComplete {
					tmpStr = regGTIDComplete.FindStringSubmatch(line)[1]
				}
				_, err := mysql.ParseGTIDSet("mysql", tmpStr)
				if err != nil {
					log.Infof("skip insert line: %s", line)
					continue
				}
				logLineMySQLPos.SubMatch[0] += " " + line
				logLineMySQLPos.SubMatch[4] += " " + line
				if tmpComplete {
					isLogLineMySQLPosComplete = true
				} else {
					log.Infof("MySQL GTID is not completed: %s", logLineMySQLPos.SubMatch[4])
					continue
				}
			}
			logLineMySQLPos.SubMatch[4] = strings.TrimRight(logLineMySQLPos.SubMatch[4], "'")
			// debug mysql position line
			jsonString, err = logLineMySQLPos.ToJSONString()
			if err != nil {
				return err
			}
			log.Infof("log line MySQL position:\n%s", jsonString)
			// unpack logLineMySQLPos.SubMatch
			var (
				binlogFile   = logLineMySQLPos.SubMatch[1]
				binlogPosStr = logLineMySQLPos.SubMatch[2]
				GTIDFlag     = logLineMySQLPos.SubMatch[3]
				GTIDSetStr   = logLineMySQLPos.SubMatch[4]
			)
			// parse binlog file position
			if binlogFile == "" {
				return fmt.Errorf("no binlog file found")
			}
			binlogPos, err := strconv.ParseUint(binlogPosStr, 10, 32)
			if err != nil {
				return err
			}
			r.BackupBinlogName = binlogFile
			r.BackupBinlogPosition = uint32(binlogPos)
			// parse gtid
			if GTIDFlag != logGTIDFlag {
				log.Infof("GTID is not on")
				continue
			}
			if GTIDSetStr == "" {
				log.Infof("GTID set is emtpy")
				continue
			}
			_, err = mysql.ParseMysqlGTIDSet(GTIDSetStr)
			if err != nil {
				return err
			}
			r.BackupGTIDSet = GTIDSetStr
			continue
		} else if logLineReplicaPos.REG.MatchString(line) || !isLogLineReplicaPosComplete {
			if isLogLineReplicaPosComplete {
				logLineReplicaPos.SubMatch = logLineReplicaPos.REG.FindStringSubmatch(line)
				if logLineReplicaPos.SubMatch[5] != "" {
					gtidSet := logLineReplicaPos.SubMatch[5]
					if regGTIDComplete.MatchString(gtidSet) {
						logLineReplicaPos.SubMatch[5] = regGTIDComplete.FindStringSubmatch(gtidSet)[1]
					} else {
						log.Infof("replica GTID is not completed: %s", logLineReplicaPos.SubMatch[5])
						isLogLineReplicaPosComplete = false
						continue
					}
				}
			} else {
				tmpStr := strings.TrimRight(line, ",")
				tmpComplete := regGTIDComplete.MatchString(line)
				if tmpComplete {
					tmpStr = regGTIDComplete.FindStringSubmatch(line)[1]
				}
				_, err := mysql.ParseGTIDSet("mysql", tmpStr)
				if err != nil {
					log.Infof("skip insert line: %s", line)
					continue
				}
				logLineReplicaPos.SubMatch[0] += " " + line
				logLineReplicaPos.SubMatch[5] += " " + line
				if tmpComplete {
					isLogLineReplicaPosComplete = true
				} else {
					log.Infof("replica GTID is not completed: %s", logLineReplicaPos.SubMatch[5])
					continue
				}
			}
			// debug replica position line
			jsonString, err = logLineReplicaPos.ToJSONString()
			if err != nil {
				return err
			}
			log.Infof("log line MySQL replica position:\n%s", jsonString)
			// unpack logLineMySQLReplicaPos.SubMatch
			var (
				masterIP            = logLineReplicaPos.SubMatch[1]
				replicaBinlogFile   = logLineReplicaPos.SubMatch[2]
				replicaBinlogPosStr = logLineReplicaPos.SubMatch[3]
				replicaGTIDFlag     = logLineReplicaPos.SubMatch[4]
				replicaGTIDSetStr   = logLineReplicaPos.SubMatch[5]
			)
			// set master ip
			r.MasterIP = masterIP
			// parse replica binlog file position
			if replicaBinlogFile == "" {
				log.Infof("binlog file is empty, may GTID is on")
			} else {
				replicaBinlogPos, err := strconv.ParseUint(replicaBinlogPosStr, 10, 32)
				if err != nil {
					return err
				}
				r.MasterBinlogName = replicaBinlogFile
				r.MasterBinlogPosition = uint32(replicaBinlogPos)
			}
			// parse replica gtid
			if replicaGTIDFlag != logReplicaGTIDFlag {
				log.Infof("master GTID is not on")
				continue
			}
			if replicaGTIDSetStr == "" {
				log.Infof("master GTID set is emtpy")
				continue
			}
			_, err = mysql.ParseGTIDSet("mysql", replicaGTIDSetStr)
			if err != nil {
				return err
			}
			r.MasterGTIDSet = replicaGTIDSetStr
			continue
		} else if logLineComplete.REG.MatchString(line) {
			logLineComplete.SubMatch = logLineComplete.REG.FindStringSubmatch(line)
			// debug complete ok line
			jsonString, err = logLineComplete.ToJSONString()
			if err != nil {
				return err
			}
			log.Infof("xtrabackup log complete:\n%s", jsonString)
			continue
		}
	}
	// verify result
	if !isLogLineMySQLPosComplete {
		return fmt.Errorf("did not parse mysql GTID set completed")
	}
	if !isLogLineReplicaPosComplete {
		return fmt.Errorf("did not parse replica GTID set completed")
	}
	if logLineToLSN.SubMatch == nil {
		return fmt.Errorf("did not find '%s'", logLineToLSN.REG)
	}
	if logLineMySQLPos.SubMatch == nil {
		return fmt.Errorf("did not find '%s'", logLineMySQLPos.REG)
	}
	if logLineComplete.SubMatch == nil {
		return fmt.Errorf("did not find '%s'", logLineComplete.REG)
	}
	return err
}

type MySQLPhysicalBackup struct {
	FileBackup
	MySQLVersion                   string             `gorm:"column:mysql_version" json:"mysql_version,omitempty"`
	InstanceID                     uint16             `json:"instance_id,omitempty"`
	BackupIP                       string             `json:"backup_ip,omitempty"`
	BackupPort                     uint16             `json:"backup_port,omitempty"`
	BackupType                     string             `json:"backup_type,omitempty"`
	BeforeBackupGTIDSet            string             `gorm:"column:before_backup_gtid_set;uniqueIndex:uk_start_gtid" json:"before_backup_gtid_set,omitempty"`
	DatetimeStart                  *utils.SQLNullTime `gorm:"uniqueIndex:uk_start_gtid" json:"datetime_start,omitempty"`
	BeforeBackupFileCount          uint32             `json:"before_backup_file_count,omitempty"`
	BeforeBackupDataSize           uint64             `json:"before_backup_data_size,omitempty"`
	BeforeBackupEarliestBinlogTime *utils.SQLNullTime `json:"before_backup_earliest_binlog_time,omitempty"`
	AfterBackupFileCount           uint32             `json:"after_backup_file_count,omitempty"`
	AfterBackupDataSize            uint64             `json:"after_backup_data_size,omitempty"`
	AfterBackupEarliestBinlogTime  *utils.SQLNullTime `json:"after_backup_earliest_binlog_time,omitempty"`
	BaseGlobalID                   uint64             `json:"base_global_id,omitempty"`
	LastGlobalID                   uint64             `json:"last_global_id,omitempty"`
	IncrementalNum                 uint16             `json:"incremental_num,omitempty"`
	Databases                      DBList             `json:"-"`
	XtrabackupResult
}

func (MySQLPhysicalBackup) TableName() string {
	return "mysql_physical_backup"
}

type MySQLBinlogBackup struct {
	FileBackup
	NextID         uint               `gorm:"index" json:"next_id,omitempty"`
	LastID         uint               `gorm:"index" json:"last_id,omitempty"`
	NextGlobalID   uint64             `gorm:"index" json:"next_global_id,omitempty"`
	LastGlobalID   uint64             `gorm:"index" json:"last_global_id,omitempty"`
	LastName       string             `json:"last_name,omitempty"`
	NextName       string             `json:"next_name,omitempty"`
	InstanceID     uint16             `json:"instance_id,omitempty"`
	MasterIP       string             `json:"master_ip,omitempty"`
	BackupIP       string             `json:"backup_ip,omitempty"`
	MasterPort     uint16             `json:"master_port,omitempty"`
	BackupPort     uint16             `json:"backup_port,omitempty"`
	DatetimeStart  *utils.SQLNullTime `gorm:"uniqueIndex:uk_start_time_gtid" json:"datetime_start,omitempty"`
	DatetimeEnd    *utils.SQLNullTime `json:"datetime_end,omitempty"`
	PositionStart  uint32             `json:"position_start,omitempty"`
	PositionEnd    uint32             `json:"position_end,omitempty"`
	GTIDSetStart   string             `gorm:"column:gtid_set_start;uniqueIndex:uk_start_time_gtid" json:"gtid_set_start,omitempty"`
	GTIDSetEnd     string             `gorm:"column:gtid_set_end" json:"gtid_set_end,omitempty"`
	EndEventType   string             `json:"end_event_type,omitempty"`
	EventCount     uint64             `json:"event_count,omitempty"`
	BinlogChecksum byte               `json:"binlog_checksum,omitempty"`
}

func (MySQLBinlogBackup) TableName() string {
	return "mysql_binlog_backup"
}
