package storage

import (
	"mybackup/utils"
	"time"
)

const MIN_EXPIRE_DURATION = 24 * time.Hour

var SupportExpireMethod = []string{"by_duration", "by_datetime"}

type Cleaner interface {
	ListExpiredByDuration(expireDuration time.Duration) (files []File, err error)
	DeleteExpiredByDuration(expireDuration time.Duration) (files []File, err error)
	ListExpiredByDatetime(expireDatetime *utils.SQLNullTime) (files []File, err error)
	DeleteExpiredByDatetime(expireDatetime *utils.SQLNullTime) (files []File, err error)
}
