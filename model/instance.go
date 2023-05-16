package model

import "mybackup/utils"

type Instance struct {
	Backup
	Arch           string             `gorm:"uniqueIndex:uk_instance" json:"arch,omitempty"`
	OSDistribution string             `gorm:"uniqueIndex:uk_instance" json:"os_distribution,omitempty"`
	DBVersion      string             `gorm:"uniqueIndex:uk_instance" json:"db_version,omitempty"`
	IP             string             `gorm:"uniqueIndex:uk_instance" json:"ip,omitempty"`
	Port           uint16             `gorm:"uniqueIndex:uk_instance" json:"port,omitempty"`
	UpTime         *utils.SQLNullTime `json:"up_time,omitempty"`
}
