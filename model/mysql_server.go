package model

import (
	"bufio"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/rand"
	"mybackup/utils"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
)

type DBList map[string][]string

func (l *DBList) Scan(val interface{}) error {
	s, ok := val.([]uint8)
	if !ok {
		return fmt.Errorf("invalid database list type")
	}
	err := json.Unmarshal([]byte(s), l)
	if err != nil {
		return err
	}
	return nil
}

func (l DBList) Value() (driver.Value, error) {
	jsonBytes, err := json.Marshal(l)
	if err != nil {
		return nil, err
	}
	return string(jsonBytes), nil
}

type Variables map[string]string

func (v *Variables) Scan(val interface{}) error {
	s, ok := val.([]uint8)
	if !ok {
		return fmt.Errorf("invalid variables")
	}
	err := json.Unmarshal([]byte(s), v)
	if err != nil {
		return err
	}
	return nil
}

func (v Variables) Value() (driver.Value, error) {
	jsonBytes, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(jsonBytes), nil
}

type ReplicaStatus map[string]any

func (r *ReplicaStatus) Scan(val interface{}) error {
	s, ok := val.([]uint8)
	if !ok {
		return fmt.Errorf("invalid variables")
	}
	err := json.Unmarshal([]byte(s), r)
	if err != nil {
		return err
	}
	return nil
}

func (r ReplicaStatus) Value() (driver.Value, error) {
	jsonBytes, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return string(jsonBytes), nil
}

func GetRandServerID() uint32 {
	var r uint32
	for {
		rand.Seed(time.Now().UnixNano())
		r = rand.Uint32()
		if r > 0 {
			log.Infof("get random ServerID: %d", r)
			return r
		}
	}
}

/* ----------------- MySQLServer -------------------------*/

type MySQLServer struct {
	Model
	PID                uint32 `gorm:"column:pid" json:"pid,omitempty"`
	MySQLDSafePath     string `gorm:"column:mysqld_safe_path" json:"mysqld_safe_path,omitempty"`
	MySQLDPath         string `gorm:"column:mysqld_path" json:"mysqld_path,omitempty"`
	ServerDefaultsFile string `gorm:"uniqueIndex:uk_cnf_version_db" json:"server_defaults_file,omitempty"`
	Version            string `gorm:"uniqueIndex:uk_cnf_version_db" json:"version,omitempty"`
	DataSize           int64  `json:"data_size,omitempty"`
	FileCount          int64  `json:"file_count,omitempty"`
	DataDIR            string `json:"data_dir,omitempty"`
	Databases          DBList `gorm:"uniqueIndex:uk_cnf_version_db" json:"databases,omitempty"`
	Status             string `json:"status,omitempty"`
	Exec               string `json:"exec,omitempty"`
	SkipCheckPIDByPort bool   `gorm:"-:all" json:"skip_check_pid_by_port,omitempty"`
	GlobalVariables    Variables
	ReplicaStatus      ReplicaStatus
	MasterServer       *MySQLServer                `gorm:"-:all"`
	MySQLClients       map[ClientType]*MySQLClient `gorm:"-:all"`
	ReplicaIORunning   bool                        `gorm:"-:all"`
	ReplicaSQLRunning  bool                        `gorm:"-:all"`
	GTIDMode           string                      `gorm:"-:all"`
	Error              string                      `json:"error,omitempty"`
	Err                error                       `gorm:"-:all" json:"-"`
}

func NewMySQLServer(adminClient *MySQLClient) (mysqlServer *MySQLServer, err error) {
	mysqlServer = &MySQLServer{
		MySQLClients: map[ClientType]*MySQLClient{
			Admin: adminClient,
		},
		Exec: "mysqld",
	}
	return mysqlServer, nil
}

func (MySQLServer) TableName() string {
	return "mysql_server"
}

func (s *MySQLServer) CollectServerInfo() (err error) {
	// get variables
	err = s.GetVariables()
	if err != nil {
		return err
	}
	// set gtid mode even for mysql 5.1
	s.SetGTIDMode()
	// set version
	if err = s.SetVersion(); err != nil {
		return err
	}
	return nil
}

func (s *MySQLServer) CollectPhysicalInfo() (err error) {
	// set data dir
	if err = s.SetDataDIR(); err != nil {
		return err
	}
	// set data size and file count
	if err = s.SetDataCount(); err != nil {
		return err
	}
	return nil
}

func (s *MySQLServer) CollectInfo() (err error) {
	// collect server info
	err = s.CollectServerInfo()
	if err != nil {
		return err
	}
	// collect physical info
	err = s.CollectPhysicalInfo()
	if err != nil {
		return err
	}
	return nil
}

func (s *MySQLServer) GetVariables() (err error) {
	for _, client := range s.MySQLClients {
		result, err := client.Execute("SHOW GLOBAL VARIABLES")
		if err != nil {
			return err
		}
		s.GlobalVariables = make(map[string]string)
		for ri := range result.Values {
			varName, err := result.GetString(ri, 0)
			if err != nil {
				s.GlobalVariables = nil
				return err
			}
			varValue, err := result.GetString(ri, 1)
			if err != nil {
				s.GlobalVariables = nil
				return err
			}
			s.GlobalVariables[varName] = varValue
		}
		return nil
	}
	return fmt.Errorf("no client to get server variables")
}

func (s *MySQLServer) GetReplicaInfo(IsGetMasterPass bool) (err error) {
	// init
	s.MasterServer = nil
	adminClient, ok := s.MySQLClients[Admin]
	if !ok || adminClient == nil {
		return fmt.Errorf("no admin client for server")
	}
	// get replica status by sql
	result, err := adminClient.Execute("SHOW SLAVE STATUS")
	if err != nil {
		return err
	}
	// check empty result
	if result.RowNumber() == 0 {
		log.Infof("no replica status found, it's not a replica server")
		return nil
	}
	s.ReplicaStatus = ReplicaStatus{}
	for name := range result.FieldNames {
		s.ReplicaStatus[name], err = result.GetStringByName(0, name)
		if err != nil {
			return fmt.Errorf("get field %s of replica status failed: %w", name, err)
		}
	}
	if s.ReplicaStatus["Slave_IO_Running"] == "Yes" {
		s.ReplicaIORunning = true
	}
	if s.ReplicaStatus["Slave_SQL_Running"] == "Yes" {
		s.ReplicaSQLRunning = true
	}
	// do not get master password
	if !IsGetMasterPass || !s.ReplicaIORunning || !s.ReplicaSQLRunning {
		return nil
	}
	//   get master_info_repository
	if s.GlobalVariables == nil {
		err = s.GetVariables()
		if err != nil {
			return err
		}
	}
	masterInfoRepository, ok := s.GlobalVariables["master_info_repository"]
	if !ok {
		masterInfoRepository = "file"
	}
	//   get master_password from table or file
	switch strings.ToUpper(masterInfoRepository) {
	case "TABLE":
		result, err := adminClient.Execute("select user_password from mysql.slave_master_info")
		if err != nil {
			return fmt.Errorf("get replica user password faied: %w", err)
		}
		masterPassword, err := result.GetStringByName(0, "user_password")
		if err != nil {
			return fmt.Errorf("get master password from table faied: %w", err)
		}
		s.ReplicaStatus["Master_Password"] = utils.Password(masterPassword)
	case "FILE":
		dataDIR, ok := s.GlobalVariables["datadir"]
		if !ok {
			return fmt.Errorf("no 'datadir' was found")
		}
		masterInfoFilePath := filepath.Join(dataDIR, "master.info")
		masterInfoFile, err := os.Open(masterInfoFilePath)
		if err != nil {
			return fmt.Errorf("open '%s' failed: %w", masterInfoFilePath, err)
		}
		defer masterInfoFile.Close()
		reader := bufio.NewReader(masterInfoFile)
		lineNum := 0
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("read file '%s' failed: %w", masterInfoFilePath, err)
			}
			lineNum++
			if lineNum == 6 {
				s.ReplicaStatus["Master_Password"] = utils.Password(strings.TrimSuffix(line, "\n"))
				break
			}
		}
	default:
		return fmt.Errorf("unsupport master_info_repository: %s", masterInfoRepository)
	}
	masterPort, err := strconv.Atoi(s.ReplicaStatus["Master_Port"].(string))
	if err != nil {
		log.Warnf("master port can not convert to int: %s", err)
	}
	replicaClient, err := NewMySQLClient(
		s.ReplicaStatus["Master_Host"].(string),
		"",
		s.ReplicaStatus["Master_User"].(string),
		"",
		uint16(masterPort),
		s.ReplicaStatus["Master_Password"].(utils.Password),
	)
	if err != nil {
		return fmt.Errorf("can not connect master with replication info")
	}
	s.MasterServer = &MySQLServer{
		MySQLClients: map[ClientType]*MySQLClient{
			Replica: replicaClient,
		},
	}
	return nil
}

// Get MySQL version
func (s *MySQLServer) SetVersion() (err error) {
	var ok bool
	s.Version, ok = s.GlobalVariables["version"]
	if !ok {
		return fmt.Errorf("get mysql server version failed")
	}
	return nil
}

// Get MySQL GTID mode
func (s *MySQLServer) SetGTIDMode() {
	var ok bool
	s.GTIDMode, ok = s.GlobalVariables["gtid_mode"]
	if !ok || strings.ToUpper(s.GTIDMode) != "ON" {
		s.GTIDMode = "OFF"
	}
}

// Get MySQL data directory
func (s *MySQLServer) SetDataDIR() (err error) {
	var ok bool
	s.DataDIR, ok = s.GlobalVariables["datadir"]
	if !ok {
		return fmt.Errorf("get mysql server datadir failed")
	}
	return nil
}

// Get MySQL data file count and size
func (s *MySQLServer) SetDataCount() (err error) {
	if s.DataDIR == "" {
		return fmt.Errorf("no datadir can not get data size")
	}
	//  query and set the datadir
	s.FileCount, s.DataSize, err = utils.DIRCount(s.DataDIR, nil)
	if err != nil {
		return fmt.Errorf("get MySQL data size failed: %w", err)
	}
	return err
}

func (s *MySQLServer) GetEventBinlog(
	ctx context.Context,
	GTIDSetStr string,
	name string,
	position uint32) (
	eventTime *utils.SQLNullTime,
	binlogName string,
	err error) {
	// get replica client of the server
	replicaClient, ok := s.MySQLClients[Replica]
	if !ok || replicaClient == nil {
		log.Infof("no replica client specified, try to use admin client instead")
		replicaClient, ok = s.MySQLClients[Admin]
		if !ok || replicaClient == nil {
			return nil, "", fmt.Errorf("no replica client")
		}
	}
	// get gtid mode
	if s.GTIDMode == "" {
		err = s.CollectServerInfo()
		if err != nil {
			return nil, "", err
		}
	}
	// get gtid and position
	replicaClient.ExcludeGTIDs = GTIDSetStr
	replicaClient.StartPosition = &mysql.Position{
		Name: name,
		Pos:  position,
	}
	// get event binlog
	log.Infof("try(1) to get binlog event info")
	for _, try := range []int{1, 2, 3} {
		eventTime, binlogName, err = replicaClient.GetEventBinlog(ctx, s.GTIDMode)
		if err == nil {
			break
		}
		sleepTime := time.Duration(try) * time.Second
		log.Infof("last try failed, sleep(%s), try(%d) to get binlog event info", sleepTime, try)
		time.Sleep(sleepTime)
	}
	return eventTime, binlogName, err
}
