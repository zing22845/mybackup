package object

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"mybackup/model"
	"mybackup/storage"
	"mybackup/utils"

	"github.com/dustin/go-humanize"
	uuid "github.com/satori/go.uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

var RegexpDDL = regexp.MustCompile(`^\s*(?i)(?:create|alter|drop|truncate|rename)`)

// Snapshot MySQL physical backup result
type BinlogStream struct {
	Type                               string                   `json:"type,omitempty"`
	Storage                            storage.Storage          `json:"storage,omitempty"`
	FlushInterval                      time.Duration            `json:"flush_interval,omitempty"`
	IsRecordDDL                        bool                     `json:"is_record_ddl,omitempty"`
	DefaultBaseDIR                     string                   `json:"-"`
	DefaultFilename                    string                   `json:"-"`
	MetaDB                             *gorm.DB                 `json:"-"`
	currentBinlogName                  string                   `json:"-"`
	currentBinlog, lastBinlog          *Binlog                  `json:"-"`
	lastWrittenEvent                   *replication.BinlogEvent `json:"-"`
	GTIDNext                           string                   `json:"-"`
	BinlogMapOffsetHighWaterMarkOfGTID map[string]uint32        `json:"-"`
	singleBinlogStream                 *Stream                  `json:"-"`
	*model.MySQLClient
}

// user defined unmarshall method
func (bs *BinlogStream) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}

	mysqlClientItems := make(map[string]json.RawMessage)
	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &bs.Type)
			if err != nil {
				return err
			}
		case "flavor", "user", "password",
			"host", "socket", "defaults_file", "port",
			"db_name", "exclude_gtids",
			"start_position", "stop_position",
			"start_datetime", "stop_datetime",
			"binlog_timeout":
			mysqlClientItems[k] = v
		case "is_record_ddl":
			err = json.Unmarshal(v, &bs.IsRecordDDL)
			if err != nil {
				return err
			}
		case "flush_interval":
			tmpStr := ""
			err = json.Unmarshal(v, &tmpStr)
			if err != nil {
				return err
			}
			bs.FlushInterval, err = time.ParseDuration(tmpStr)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}

	// unmarshal MySQL client
	mysqlClientData, err := json.Marshal(mysqlClientItems)
	if err != nil {
		return err
	}
	err = json.Unmarshal(mysqlClientData, &bs.MySQLClient)
	if err != nil {
		return err
	}

	if bs.FlushInterval == 0 {
		bs.FlushInterval = 10 * time.Second
	}
	bs.BinlogMapOffsetHighWaterMarkOfGTID = make(map[string]uint32)
	return nil
}

func (bs *BinlogStream) GetType() string {
	return bs.Type
}

// Move
func (bs *BinlogStream) Move(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport operation now")
}

// Upload
func (bs *BinlogStream) Upload(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport operation now")
}

// Download
func (bs *BinlogStream) Download(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport operation now")
}

// Parse extra events for RAW mode syncer except query event and gtid event for performance reason
func (bs *BinlogStream) onExtraEvent(ev *replication.BinlogEvent) error {
	switch typedEvent := ev.Event.(type) {
	case *replication.FormatDescriptionEvent:
		log.Infof("set format description event for extra parser: %+v", ev.Event)
	case *replication.PreviousGTIDsEvent:
		if typedEvent.GTIDSets != "" {
			for _, subGTIDSet := range strings.Split(typedEvent.GTIDSets, ",") {
				err := bs.PurgedGTIDSet.Update(subGTIDSet)
				if err != nil {
					return err
				}
			}
		}
		bs.currentBinlog.GTIDSetStart = bs.PurgedGTIDSet.String()
		bs.currentBinlog.GTIDSetEnd = bs.PurgedGTIDSet.String()
	default:
		return fmt.Errorf("unsupport event type %T", ev.Event)
	}
	return nil
}

// Backup backup Binlog to another Object
// nolint:gocyclo
func (bs *BinlogStream) Backup(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	targetBinlogSet, ok := (targetObject).(*BinlogSet)
	if !ok {
		return fmt.Errorf("only support BinlogSet as target now")
	}
	if targetBinlogSet.DefaultStorage == nil {
		return fmt.Errorf("no default storage for binlog set found")
	}
	// get default timeout
	if bs.BinlogTimeout == 0 {
		// default 1.5 days no event is timeout
		bs.BinlogTimeout = 36 * time.Hour
	}
	// inite metadb
	err = bs.MetaDB.AutoMigrate(&model.MySQLBinlogBackup{}, &model.MySQLDDLBackup{})
	if err != nil {
		return err
	}
	// get streamer
	streamer, err := bs.GetBinlogStreamer(true, "ON")
	if err != nil {
		return err
	}
	targetBinlogSet.DefaultMasterIP = bs.Host
	targetBinlogSet.DefaultMasterPort = bs.Port

	// parse exclude gtid
	bs.PurgedGTIDSet, err = mysql.ParseGTIDSet(bs.Flavor, bs.ExcludeGTIDs)
	if err != nil {
		return err
	}
	// init binlog parser for parsing no RawMode event
	xtraBinlogParser := replication.NewBinlogParser()
	xtraBinlogParser.SetFlavor(bs.Flavor)
	xtraBinlogParser.SetRawMode(false)

	// backup binlogs
	for {
		eventCTX, cancel := context.WithTimeout(ctx, bs.BinlogTimeout)
		ev, err := streamer.GetEvent(eventCTX)
		cancel()
		if err != nil {
			return err
		}

		switch ev.Header.EventType {
		case replication.ROTATE_EVENT:
			if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
				// cache the last binlog name and get next binlog name
				rotateEvent := ev.Event.(*replication.RotateEvent)
				bs.currentBinlogName = string(rotateEvent.NextLogName)
				log.Infof("(%s) skip write fake rotate event", bs.currentBinlogName)
				continue
			} else {
				if bs.CheckSkipWrite(ev) {
					continue
				}
				// set meta
				bs.currentBinlog.Status = "finished"
				// write data
				err = bs.WriteData(bs.currentBinlog, ev)
				if err != nil {
					return err
				}
				// write meta
				err = bs.WriteMeta(bs.currentBinlog, ev)
				if err != nil {
					return err
				}
				// log end event type
				log.Infof("(%s) end with %s",
					bs.currentBinlog.OriginName,
					bs.currentBinlog.EndEventType,
				)
			}
		case replication.STOP_EVENT:
			if bs.CheckSkipWrite(ev) {
				continue
			}
			bs.currentBinlog.Status = "finished"
			// write data
			err = bs.WriteData(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
			// write meta
			err = bs.WriteMeta(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
			// log end event type
			log.Infof("(%s) end with %s",
				bs.currentBinlog.OriginName,
				bs.currentBinlog.EndEventType,
			)
		case replication.FORMAT_DESCRIPTION_EVENT:
			_, err := xtraBinlogParser.ParseSingleEvent(bytes.NewReader(ev.RawData), bs.onExtraEvent)
			if err != nil {
				return err
			}
			if bs.CheckSkipWrite(ev) {
				continue
			}
			// check abnormal binlog name
			if len(bs.currentBinlogName) == 0 {
				return fmt.Errorf("empty binlog filename for FormateDescriptionEvent")
			}
			// FormateDescriptionEvent is the first event in binlog, we will close old stream and create a stream
			if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
				return fmt.Errorf("(%s) abnormal format_description_event, timestamp: %d, offset: %d",
					bs.currentBinlogName,
					ev.Header.Timestamp,
					ev.Header.LogPos)
			}

			// done last stream
			if bs.singleBinlogStream != nil {
				bs.lastBinlog = bs.currentBinlog
				// current stream without binlog
				if bs.lastBinlog == nil {
					return fmt.Errorf("(%s) streaming without binlog", bs.currentBinlogName)
				}
				// done stream
				bs.singleBinlogStream.Cancel()
				bs.singleBinlogStream.WaitGroup.Wait()
				if bs.singleBinlogStream.Err != nil {
					return bs.singleBinlogStream.Err
				}

				// fix last binlog meta without rotate event and stop event
				if bs.lastBinlog.EndEventType != replication.ROTATE_EVENT.String() && bs.lastBinlog.EndEventType != replication.STOP_EVENT.String() {
					lastGTIDOffset, err := mysql.ParseGTIDSet(bs.Flavor, bs.lastBinlog.GTIDSetEnd)
					if err != nil {
						return err
					}
					if !bs.PurgedGTIDSet.Equal(lastGTIDOffset) {
						// TODO: write lost binlog and continue
						return fmt.Errorf("(%s ~ %s) lost GTID between (%s) and (%s)",
							bs.lastBinlog.OriginName,
							bs.currentBinlogName,
							bs.lastBinlog.GTIDSetEnd,
							bs.PurgedGTIDSet.String())
					}
					bs.lastBinlog.Status = "finished"
					// log end event type
					log.Infof("(%s) end without ROTATE_EVENT or STOP_EVENT but %s",
						bs.lastBinlog.OriginName,
						bs.lastBinlog.EndEventType,
					)
				}
				err = bs.WriteMeta(bs.lastBinlog, bs.lastWrittenEvent)
				if err != nil {
					return err
				}
				// print last binlog upload result
				log.Infof("(%s) finished store(%s), gtid(%s ~ %s), size: %d, original md5sum: %s, stored size: %d, stored md5sum: %s",
					bs.lastBinlog.OriginName,
					bs.lastBinlog.FilePath,
					bs.lastBinlog.GTIDSetStart,
					bs.lastBinlog.GTIDSetEnd,
					bs.lastBinlog.OriginSize,
					bs.lastBinlog.OriginMD5Sum,
					bs.lastBinlog.Size,
					bs.lastBinlog.MD5Sum)
			}

			// get checksum algorithm for parsing DDL in query event
			formatDescriptionEvent, ok := ev.Event.(*replication.FormatDescriptionEvent)
			if !ok {
				return fmt.Errorf("(%s) convert format description event failed",
					bs.currentBinlogName)
			}

			// new binlog backup file
			bs.currentBinlog, err = targetBinlogSet.NewBinlog(
				bs.currentBinlogName,
				bs.lastBinlog,
				ev.Header.Timestamp,
				formatDescriptionEvent.ChecksumAlgorithm)
			if err != nil {
				return err
			}
			bs.currentBinlog.SpeedLimit = targetBinlogSet.DefaultSpeedLimit
			// set GTID start
			bs.currentBinlog.GTIDSetStart = bs.PurgedGTIDSet.String()
			// log file path
			log.Infof("(%s) starting store(%s) from %s",
				bs.currentBinlog.OriginName,
				bs.currentBinlog.FilePath,
				bs.currentBinlog.GTIDSetStart,
			)
			// new stream
			bs.singleBinlogStream = NewStream(ctx, bs.FlushInterval, bs.currentBinlog.OriginName)
			bs.singleBinlogStream.InputSpeedLimit = bs.currentBinlog.SpeedLimit
			bs.singleBinlogStream.CompressMethod = bs.currentBinlog.CompressMethod
			if bs.currentBinlog.CompressMethod == "" {
				bs.singleBinlogStream.InputSize = 100 * 1024 * 1024 * 1024 // 100GB -> partSize = 10MB
			} else {
				bs.singleBinlogStream.InputSize = 50 * 1024 * 1024 * 1024 // 50GB - partsize = 5MB
			}
			bs.singleBinlogStream.OutputSpeedLimit = bs.currentBinlog.SpeedLimit
			// start store stream to binlog backup file
			err = bs.singleBinlogStream.Store(u, bs.currentBinlog.Storage, bs.DefaultFilename, overwrite)
			if err != nil {
				return err
			}
			// write binlog file header: fe'bin'
			n, err := bs.singleBinlogStream.InputWriter.Write(replication.BinLogFileHeader)
			if err != nil {
				return fmt.Errorf("(%s) write binlog file header failed: %w", bs.currentBinlog.OriginName, err)
			} else if n != len(replication.BinLogFileHeader) {
				return fmt.Errorf("(%s) write binlog file header failed: %w", bs.currentBinlog.OriginName, io.ErrShortWrite)
			}
			bs.currentBinlog.OriginSize += int64(n)
			// write data and meta
			err = bs.WriteData(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
			err = bs.WriteMeta(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
			// flush file header to check if storage is ok
			bs.singleBinlogStream.Flush()
			if bs.singleBinlogStream.Err != nil {
				return fmt.Errorf("(%s) flush writer failed %w", bs.currentBinlog.OriginName, bs.singleBinlogStream.Err)
			}
			log.Infof("(%s) init write data and meta success", bs.currentBinlog.OriginName)
			// update last binlog meta
			if bs.lastBinlog != nil {
				bs.lastBinlog.NextName = bs.currentBinlog.OriginName
				bs.lastBinlog.NextID = bs.currentBinlog.ID
				bs.MetaDB.Clauses(
					clause.OnConflict{
						Columns: []clause.Column{
							{Name: "datetime_start"},
							{Name: "gtid_set_start"},
						},
						UpdateAll: true,
					}).Save(&bs.lastBinlog.MySQLBinlogBackup)
				if bs.MetaDB.Error != nil {
					return bs.MetaDB.Error
				}
				log.Infof("(%s) update next info success", bs.lastBinlog.OriginName)
			}
		case replication.PREVIOUS_GTIDS_EVENT:
			if bs.CheckSkipWrite(ev) {
				continue
			}
			_, err := xtraBinlogParser.ParseSingleEvent(bytes.NewReader(ev.RawData), bs.onExtraEvent)
			if err != nil {
				return err
			}
			// write data and meta
			err = bs.WriteData(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
			err = bs.WriteMeta(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
			log.Infof("(%s) write %s success: %s",
				bs.currentBinlog.OriginName,
				bs.currentBinlog.EndEventType,
				bs.currentBinlog.GTIDSetEnd)
		case replication.GTID_EVENT:
			if bs.CheckSkipWrite(ev) {
				continue
			}
			// parse gtid event
			err = bs.parseGTIDEvent(ev)
			if err != nil {
				return err
			}
			// flush CompressedWriter buffer and  write meta every tick
			select {
			case <-bs.singleBinlogStream.Ticker.C:
				// flush compressedWriter buffer before write new transaction
				bs.singleBinlogStream.Flush()
				if bs.singleBinlogStream.Err != nil {
					return fmt.Errorf("(%s) flush writer before GTID event (%s) failed %w",
						bs.currentBinlog.OriginName, bs.currentBinlog.GTIDSetEnd, bs.singleBinlogStream.Err)
				}
				// write meta every tick
				err = bs.WriteMeta(bs.currentBinlog, ev)
				if err != nil {
					return err
				}
				log.Infof("(%s) flushed: %s, %s: %s",
					bs.currentBinlog.OriginName,
					humanize.Bytes(uint64(bs.singleBinlogStream.compressedWriter.UncompressedSize())),
					bs.currentBinlog.EndEventType,
					bs.currentBinlog.GTIDSetEnd)
			default:
			}
			// update GTID
			nextGTIDEvent, ok := ev.Event.(*replication.GTIDEvent)
			if !ok {
				return fmt.Errorf("(%s) convert gtid event failed", bs.currentBinlog.OriginName)
			}
			u, err := uuid.FromBytes(nextGTIDEvent.SID)
			if err != nil {
				return err
			}
			bs.GTIDNext = fmt.Sprintf("%s:%d", u, nextGTIDEvent.GNO)
			err = bs.PurgedGTIDSet.Update(bs.GTIDNext)
			if err != nil {
				return err
			}
			bs.currentBinlog.GTIDSetEnd = bs.PurgedGTIDSet.String()
			// write data and meta
			err = bs.WriteData(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
			// reinit GTIDBinlogOffsetMap for current GTID
			bs.BinlogMapOffsetHighWaterMarkOfGTID = make(map[string]uint32)
		case replication.HEARTBEAT_EVENT:
			continue
		case replication.QUERY_EVENT:
			if bs.CheckSkipWrite(ev) {
				continue
			}
			// record DDL
			if bs.IsRecordDDL {
				err = bs.recordDDL(ev)
				if err != nil {
					return err
				}
			}
			// write data
			err = bs.WriteData(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
		default:
			if bs.CheckSkipWrite(ev) {
				continue
			}
			// write data
			err = bs.WriteData(bs.currentBinlog, ev)
			if err != nil {
				return err
			}
		}
	}
}

func (bs *BinlogStream) CheckSkipWrite(ev *replication.BinlogEvent) (skip bool) {
	/*
		Background:
		1. every time the binlog syncing connection is broken,
		   the syncer will try to reconnect mysql from current uncomplished transaction(bs.GTIDNext)
		2. we use bs.GTIDBinlogOffsetMap to record each binlog's high-water mark offset of current transaction
		3. if we found the current event offset lower than the high-water mark, we skip the event data write and meta write

		summerize: the re-sync unit is transaction, but the write binlog unit is event,
		   we use this method to track which event had been written and should be skipped if re-syncing is happening
	*/
	if writtenOffset, ok := bs.BinlogMapOffsetHighWaterMarkOfGTID[bs.currentBinlogName]; ok && writtenOffset >= ev.Header.LogPos {
		log.Infof("position(%s:%d) had been written, skip until beyond high-water mark: %d",
			bs.currentBinlogName, ev.Header.LogPos, writtenOffset)
		return true
	}
	return false
}

func (bs *BinlogStream) getEventBody(raw []byte) (body []byte) {
	if bs.currentBinlog.BinlogChecksum == replication.BINLOG_CHECKSUM_ALG_CRC32 {
		body = raw[replication.EventHeaderSize : len(raw)-replication.BinlogChecksumLength]
	} else {
		body = raw[replication.EventHeaderSize:]
	}
	return body
}

func (bs *BinlogStream) parseGTIDEvent(ev *replication.BinlogEvent) (err error) {
	GTIDEvent := &replication.GTIDEvent{}
	body := bs.getEventBody(ev.RawData)
	if err := GTIDEvent.Decode(body); err != nil {
		return &replication.EventError{
			Header: ev.Header,
			Err:    err.Error(),
			Data:   body}
	}
	ev.Event = GTIDEvent
	return nil
}

func (bs *BinlogStream) recordDDL(ev *replication.BinlogEvent) (err error) {
	queryEvent := &replication.QueryEvent{}
	body := bs.getEventBody(ev.RawData)
	if err := queryEvent.Decode(body); err != nil {
		return &replication.EventError{
			Header: ev.Header,
			Err:    err.Error(),
			Data:   body}
	}
	schema := string(queryEvent.Schema)
	query := string(queryEvent.Query)
	if RegexpDDL.MatchString(query) {
		ev.Event = queryEvent
		ddlBackup := model.NewMySQLDDLBackup(ev.Header.Timestamp, bs.GTIDNext, schema, query)
		ddlBackup.ExecuteTime = queryEvent.ExecutionTime
		ddlBackup.BinlogName = bs.currentBinlog.OriginName
		ddlBackup.StartLogPosition = bs.lastWrittenEvent.Header.LogPos
		ddlBackup.EndLogPosition = ev.Header.LogPos
		ddlBackup.Status = "finished"
		bs.MetaDB.Clauses(
			clause.OnConflict{
				Columns: []clause.Column{
					{Name: "event_datetime"},
					{Name: "gtid"},
				},
				UpdateAll: true,
			}).Save(ddlBackup)
		if bs.MetaDB.Error != nil {
			return bs.MetaDB.Error
		}
	}
	return nil
}

func (bs *BinlogStream) WriteData(binlog *Binlog, ev *replication.BinlogEvent) (err error) {
	// write to pipe writer
	n, err := bs.singleBinlogStream.InputWriter.Write(ev.RawData)
	if err != nil {
		return err
	} else if n != len(ev.RawData) {
		return fmt.Errorf("(%s) event size: %d, write size: %d, err: %w",
			binlog.OriginName, len(ev.RawData), n, io.ErrShortWrite)
	}
	// calculate the writen size
	binlog.OriginSize += int64(n)
	// set offset
	binlog.PositionEnd = ev.Header.LogPos
	// check writen size by cumulative size and offset, may need remove this check when huge binlog size exceeds max offset
	if binlog.OriginSize != int64(binlog.PositionEnd) {
		return fmt.Errorf("(%s) written size %d not match offset %d",
			binlog.OriginName, binlog.OriginSize, binlog.PositionEnd)
	}
	bs.BinlogMapOffsetHighWaterMarkOfGTID[binlog.OriginName] = binlog.PositionEnd
	binlog.EventCount += 1
	bs.lastWrittenEvent = ev
	return bs.singleBinlogStream.Err
}

func (bs *BinlogStream) WriteMeta(binlog *Binlog, ev *replication.BinlogEvent) error {
	if ev != nil {
		binlog.EndEventType = ev.Header.EventType.String()
		binlog.DatetimeEnd = utils.NewTime(time.Unix(int64(ev.Header.Timestamp), 0))
	}
	binlog.BackupEndTime = utils.NewTime(time.Now())
	bs.MetaDB.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{
				{Name: "datetime_start"},
				{Name: "gtid_set_start"},
			},
			UpdateAll: true,
		}).Save(&binlog.MySQLBinlogBackup)
	if bs.MetaDB.Error != nil {
		return bs.MetaDB.Error
	}
	// log.Debugf("(%s) write meta of %s success", bs.currentBinlog.OriginName, binlog.EndEventType)
	return nil
}

func (bs *BinlogStream) GetStorageFilePath() string {
	log.Infof("not support yet")
	return ""
}

// Recover recover from Snapshot to another snapshot
func (bs *BinlogStream) Recover(ctx context.Context, u utils.Utils, targetObject Object, overwrite bool) (err error) {
	return fmt.Errorf("unsupport operation now")
}

func (bs *BinlogStream) Prepare(ctx context.Context, u utils.Utils) (err error) {
	return fmt.Errorf("unsupport operation now")
}

func (bs *BinlogStream) SetStorageFilePath(filePath string) {
	bs.Storage.SetFilePath(filePath)
}

func (bs *BinlogStream) SetDefaultBaseDIR(baseDIR string) (err error) {
	log.Infof("set default base directory: %s", baseDIR)

	if baseDIR == "" {
		return fmt.Errorf("can not set empty DefaultBaseDIR")
	}
	bs.DefaultBaseDIR = baseDIR
	return nil
}

func (bs *BinlogStream) SetDefaultFileName(filename string) (err error) {
	log.Infof("set default file name: %s", filename)

	if filename == "" {
		return fmt.Errorf("can not set empty DefaultFilename")
	}
	bs.DefaultFilename = filename
	return nil
}

// List expired snapshot
func (bs *BinlogStream) List(ctx context.Context, utils utils.Utils) (err error) {
	return fmt.Errorf("not support")
}

func (bs *BinlogStream) Parse(u utils.Utils) (err error) {
	return fmt.Errorf("unsupport operation now")
}

func (bs *BinlogStream) SetMetaDB(MetaDB *gorm.DB) (err error) {
	bs.MetaDB = MetaDB
	return nil
}
