package model

import (
	"time"
)

type TCP struct {
	IP          string        `json:"ip,omitempty"`
	Port        uint16        `json:"port,omitempty"`
	Password    string        `gorm:"-:all" json:"password,omitempty"`
	ConnTimeout time.Duration `json:"conn_timeout,omitempty"`
	SendSize    int64         `json:"send_size,omitempty"`
	ReceiveSize int64         `json:"receive_size,omitempty"`
}
