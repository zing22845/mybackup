package utils

// Utils backup or recover tools
type Utils struct {
	Xtrabackup *Xtrabackup `json:"xtrabackup,omitempty"`
	Xbstream   *Xbstream   `json:"xbstream,omitempty"`
	PgBackRest *PgBackRest `json:"pgbackrest,omitempty"`
}
