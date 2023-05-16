package model

import (
	"context"
	"encoding/json"
	"fmt"
	"mybackup/utils"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	log "github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
)

type ClientType uint8

const (
	Admin ClientType = iota
	Replica
)

/* ----------------- MySQLClient -------------------------*/

type MySQLClient struct {
	Flavor           string          `json:"flavor,omitempty"`
	DefaultsFile     string          `json:"defaults_file,omitempty"`
	User             string          `json:"user,omitempty"`
	Password         utils.Password  `json:"password,omitempty"`
	Socket           string          `json:"socket,omitempty"`
	Host             string          `json:"host,omitempty"`
	Port             uint16          `json:"port,omitempty"`
	Addr             string          `json:"addr,omitempty"`
	DBName           string          `json:"db_name,omitempty"`
	ExcludeGTIDs     string          `json:"exclude_gtids,omitempty"`
	StartPosition    *mysql.Position `json:"start_position,omitempty"`
	StopPosition     *mysql.Position `json:"stop_position,omitempty"`
	StartDatetime    *time.Time      `json:"start_datetime,omitempty"`
	StopDatetime     *time.Time      `json:"stop_datetime,omitempty"`
	BinlogTimeout    time.Duration   `json:"binlog_timeout,omitempty"`
	PurgedGTIDSet    mysql.GTIDSet   `json:"-"`
	Conn             *client.Conn    `json:"-"`
	SessionVariables Variables       `json:"-"`
}

func NewMySQLClient(host, socket, user, DBName string, port uint16, password utils.Password) (mysqlClient *MySQLClient, err error) {
	mysqlClient = &MySQLClient{
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
		Socket:   socket,
		DBName:   DBName,
		Flavor:   "mysql",
	}
	err = mysqlClient.Connect()
	return mysqlClient, err
}

func (c *MySQLClient) UnmarshalJSON(data []byte) (err error) {
	// default value
	c.Flavor = "mysql"

	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	for k, v := range items {
		switch k {
		case "flavor":
			err = json.Unmarshal(v, &c.Flavor)
			if err != nil {
				return err
			}
		case "host":
			err = json.Unmarshal(v, &c.Host)
			if err != nil {
				return err
			}
		case "port":
			err = json.Unmarshal(v, &c.Port)
			if err != nil {
				return err
			}
		case "user":
			err = json.Unmarshal(v, &c.User)
			if err != nil {
				return err
			}
		case "password":
			err = json.Unmarshal(v, &c.Password)
			if err != nil {
				return err
			}
		case "socket":
			err = json.Unmarshal(v, &c.Socket)
			if err != nil {
				return err
			}
		case "defaults_file":
			err = json.Unmarshal(v, &c.DefaultsFile)
			if err != nil {
				return err
			}
		case "exclude_gtids":
			err = json.Unmarshal(v, &c.ExcludeGTIDs)
			if err != nil {
				return err
			}
		case "start_position":
			err = json.Unmarshal(v, &c.StartPosition)
			if err != nil {
				return err
			}
		case "start_datetime":
			err = json.Unmarshal(v, &c.StartDatetime)
			if err != nil {
				return err
			}
		case "binlog_timeout":
			tmpStr := ""
			err = json.Unmarshal(v, &tmpStr)
			if err != nil {
				return err
			}
			c.BinlogTimeout, err = time.ParseDuration(tmpStr)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	// read defaults-file
	if c.DefaultsFile != "" {
		err = c.ReadDefaultsFile()
		if err != nil {
			return err
		}
	}
	return
}

func (c *MySQLClient) ReadDefaultsFile() (err error) {
	cfg, err := ini.Load(c.DefaultsFile)
	if err != nil {
		return fmt.Errorf(
			"failed to read defualts file(%s): %w",
			c.DefaultsFile, err,
		)
	}
	if cfg.Section("client") == nil {
		return fmt.Errorf(
			"defaults file %s has no section client",
			c.DefaultsFile,
		)
	}
	if cfg.Section("client").HasKey("user") && c.User == "" {
		c.User = cfg.Section("client").Key("user").String()
	}
	if cfg.Section("client").HasKey("password") && c.Password == "" {
		c.Password = utils.Password(cfg.Section("client").Key("password").String())
	}
	if cfg.Section("client").HasKey("socket") && c.Socket == "" {
		c.Socket = cfg.Section("client").Key("socket").String()
	}
	if cfg.Section("client").HasKey("host") && c.Host == "" {
		c.Host = cfg.Section("client").Key("host").String()
	}
	if cfg.Section("client").HasKey("port") && c.Port == 0 {
		port, err := cfg.Section("client").Key("port").Uint()
		if err != nil {
			return fmt.Errorf("parse port in config file(%s) failed: %w",
				c.DefaultsFile, err)
		}
		c.Port = uint16(port)
	}
	return nil
}

func (c *MySQLClient) Connect() (err error) {
	if c.Port == 0 {
		c.Addr = c.Socket
	} else {
		c.Addr = fmt.Sprintf("%s:%d", c.Host, c.Port)
	}
	if c.Conn == nil || c.Conn.Ping() != nil {
		cJSON, err := json.MarshalIndent(c, "", "  ")
		if err != nil {
			return err
		}
		log.Debugf("connect MySQL with client:\n%s", cJSON)
		c.Conn, err = client.Connect(c.Addr, c.User, string(c.Password), c.DBName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *MySQLClient) Execute(cmd string) (result *mysql.Result, err error) {
	err = c.Connect()
	if err != nil {
		return nil, err
	}
	return c.Conn.Execute(cmd)
}

// GetBinlogStreamer
func (c *MySQLClient) GetBinlogStreamer(rawMode bool, gtidMode string) (streamer *replication.BinlogStreamer, err error) {
	// parse GTID set
	var GTIDSet mysql.GTIDSet
	if gtidMode == "ON" {
		GTIDSet, err = mysql.ParseGTIDSet(c.Flavor, c.ExcludeGTIDs)
		if err != nil {
			return nil, err
		}
	}
	// init event time
	if c.Port == 0 {
		c.Host = c.Socket
	}
	// init syncer
	cfg := replication.BinlogSyncerConfig{
		ServerID:             GetRandServerID(), // random server_id range [1, 2^32)
		Flavor:               c.Flavor,
		Host:                 c.Host,
		Port:                 c.Port,
		User:                 c.User,
		Password:             string(c.Password),
		RawModeEnabled:       rawMode,
		MaxReconnectAttempts: 10,
		Logger:               log.StandardLogger(),
	}
	syncer := replication.NewBinlogSyncer(cfg)
	// init streamer
	if GTIDSet != nil {
		log.Infof("get event by GTIDSet: %s", GTIDSet)
		streamer, err = syncer.StartSyncGTID(GTIDSet)
	} else if c.StartPosition != nil {
		log.Infof("get event by file position: %s", *c.StartPosition)
		streamer, err = syncer.StartSync(*c.StartPosition)
	} else {
		err = fmt.Errorf("both GTID and Position is empty")
	}
	if err != nil {
		return nil, err
	}
	return streamer, nil
}

/*
Fake a replica to retrive master event time and position
Input:

	GTIDSetStr: the gtid set has been executed
	pos: excuted binlog's end position

if both GTIDSet and nextPos are nil, use current time as event end time, initial binlog position as end position

Output:

	eventTime: use the next query event's start time as the event time, because its hard to get the accurate end time of last executed event
	eventPos:
		if GTIDSet is provided, use current binlog 0 position
		if pos is provided, use the pos as eventPos

if no more events with 5 seconds timeout, use current time as the event time
*/
func (c *MySQLClient) GetEventBinlog(ctx context.Context, gtidMode string) (eventTime *utils.SQLNullTime, binlogName string, err error) {
	// get streamer
	streamer, err := c.GetBinlogStreamer(true, gtidMode)
	if err != nil {
		return nil, "", err
	}
	eventTime = nil

	// get event time
	eventCount := 0
	for {
		eventCTX, cancel := context.WithTimeout(ctx, 5*time.Second)
		ev, err := streamer.GetEvent(eventCTX)
		cancel()
		eventCount++
		if ev != nil {
			if binlogName == "" && eventCount == 1 &&
				ev.Header.EventType == replication.ROTATE_EVENT {
				rotateEvent, ok := (ev.Event).(*replication.RotateEvent)
				if !ok {
					return nil, "", fmt.Errorf("first event not a fake rotate event")
				}
				binlogName = string(rotateEvent.NextLogName)
			}
			if ev.Header.EventType == replication.QUERY_EVENT {
				log.Infof("find next event with header: %v\n", ev.Header)
				eventTime = utils.NewTime(time.Unix(int64(ev.Header.Timestamp), 0))
				return eventTime, binlogName, nil
			}
		}
		switch err {
		case context.DeadlineExceeded:
			if eventCount <= 1 {
				return nil, "", fmt.Errorf("read event timeout")
			}
			eventTime = utils.NewTime(time.Now().Local())
			log.Infof("retrive event timeout, use current time: %s", eventTime.Time)
			return eventTime, binlogName, nil
		case nil:
			continue
		default:
			return nil, "", err
		}
	}
}
