package object

import (
	"context"
	"mybackup/utils"

	"gorm.io/gorm"
)

// Object backup or recovery source or target
type Object interface {
	UnmarshalJSON(data []byte) (err error)
	GetType() string
	Move(ctx context.Context, utils utils.Utils, targetObject Object, overwrite bool) (err error)
	Upload(ctx context.Context, utils utils.Utils, targetObject Object, overwrite bool) (err error)
	Download(ctx context.Context, utils utils.Utils, targetObject Object, overwrite bool) (err error)
	Backup(ctx context.Context, utils utils.Utils, targetObject Object, overwrite bool) (err error)
	Recover(ctx context.Context, utils utils.Utils, targetObject Object, overwrite bool) (err error)
	Prepare(ctx context.Context, utils utils.Utils) (err error)
	List(ctx context.Context, utils utils.Utils) (err error)
	GetStorageFilePath() string
	SetStorageFilePath(filePath string)
	SetDefaultBaseDIR(baseDIR string) (err error)
	SetDefaultFileName(filename string) (err error)
	SetMetaDB(MetaDB *gorm.DB) (err error)
}
