package model

import (
	"mybackup/utils"
	"time"
)

type Backup struct {
	Model
	TCP
	GlobalID       uint64                  `gorm:"index" json:"global_id,omitempty"`
	Status         string                  `gorm:"index" json:"status,omitempty"`
	ExpireMethod   string                  `json:"expire_method,omitempty"`
	ExpireDatetime *utils.SQLNullTime      `json:"expire_datetime,omitempty"`
	ExpireDuration time.Duration           `json:"expire_duration,omitempty"`
	Error          string                  `json:"error,omitempty"`
	Err            error                   `gorm:"-:all" json:"-"`
	Timers         map[string]*utils.Timer `gorm:"-:all" json:"timers,omitempty"`
}

type FileBackup struct {
	Backup
	PartID            uint16             `json:"part_id,omitempty"`
	PartCount         uint16             `json:"part_count,omitempty"`
	OriginName        string             `json:"origin_name,omitempty"`
	OriginFileType    string             `json:"origin_file_type,omitempty"`
	OriginSize        int64              `json:"origin_size,omitempty"`
	OriginCount       int64              `json:"origin_count,omitempty"`
	OriginMD5Sum      string             `json:"origin_md5_sum,omitempty"`
	FilePath          string             `json:"file_path,omitempty"`
	FileType          string             `json:"file_type,omitempty"`
	Size              int64              `json:"size,omitempty"`
	Count             int64              `json:"count,omitempty"`
	MD5Sum            string             `json:"md5_sum,omitempty"`
	CompressMethod    string             `json:"compress_method,omitempty"`
	DecompressMethod  string             `json:"decompress_method,omitempty"`
	PackMethod        string             `json:"pack_method,omitempty"`
	UnpackMethod      string             `json:"unpack_method,omitempty"`
	EncryptMethod     string             `json:"encrypt_method,omitempty"`
	EncryptKey        string             `json:"encrypt_key,omitempty"`
	DecryptMethod     string             `json:"decrypt_method,omitempty"`
	DecryptKey        string             `json:"decrypt_key,omitempty"`
	BaseDIR           string             `json:"base_dir,omitempty"`
	SpeedLimit        string             `json:"speed_limit,omitempty"`
	BackupStartTime   *utils.SQLNullTime `json:"backup_start_time,omitempty"`
	BackupEndTime     *utils.SQLNullTime `json:"backup_end_time,omitempty"`
	RecoverStartTime  *utils.SQLNullTime `json:"recover_start_time,omitempty"`
	RecoverEndTime    *utils.SQLNullTime `json:"recover_end_time,omitempty"`
	DownloadStartTime *utils.SQLNullTime `json:"download_start_time,omitempty"`
	DownloadEndTime   *utils.SQLNullTime `json:"download_end_time,omitempty"`
	PrepareStartTime  *utils.SQLNullTime `json:"prepare_start_time,omitempty"`
	PrepareEndTime    *utils.SQLNullTime `json:"prepare_end_time,omitempty"`
}
