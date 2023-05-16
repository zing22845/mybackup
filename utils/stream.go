package utils

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/juju/ratelimit"
	"github.com/machinebox/progress"
	log "github.com/sirupsen/logrus"
)

// Create a limited writer
func LimitWriter(w io.Writer, speedLimitStr string) (limitedWriter io.Writer, err error) {
	speedLimit, err := humanize.ParseBytes(speedLimitStr)
	if err != nil {
		return nil, fmt.Errorf("parse speed limit(%s) failed: %w",
			speedLimitStr, err)
	}
	if speedLimit <= 0 {
		return nil, fmt.Errorf("invalid speed limit(%d), must > 0", speedLimit)
	}
	speedLimitBucket := ratelimit.NewBucketWithRate(float64(speedLimit), int64(speedLimit))
	limitedWriter = ratelimit.Writer(w, speedLimitBucket)
	return limitedWriter, nil
}

// Create a limited reader
func LimitReader(r io.Reader, speedLimitStr string) (limitedReader io.Reader, err error) {
	speedLimit, err := humanize.ParseBytes(speedLimitStr)
	if err != nil {
		return nil, fmt.Errorf("parse speed limit(%s) failed: %w",
			speedLimitStr, err)
	}
	if speedLimit <= 0 {
		return nil, fmt.Errorf("invalid speed limit(%d), must > 0", speedLimit)
	}
	speedLimitBucket := ratelimit.NewBucketWithRate(float64(speedLimit), int64(speedLimit))
	limitedReader = ratelimit.Reader(r, speedLimitBucket)
	return limitedReader, nil
}

// Create a writer with progress
func NewProgressWriter(ctx context.Context, w io.Writer, logPrefix string, totalSize int64, timer *Timer, interval time.Duration) io.Writer {
	progressWriter := progress.NewWriter(w)
	// Start a goroutine printing progress
	go func() {
		progressChan := progress.NewTicker(ctx, progressWriter, totalSize, interval)
		for p := range progressChan {
			speed := float64(p.N()) / timer.ElapsedSeconds()
			if totalSize >= 0 {
				log.Infof(
					"(%s) write %s/%s, %.2f pct, %s/s, %v remaining...",
					logPrefix,
					humanize.Bytes(uint64(p.N())),
					humanize.Bytes(uint64(p.Size())),
					p.Percent(),
					humanize.Bytes(uint64(speed)),
					p.Remaining().Round(time.Second),
				)
			} else {
				log.Infof(
					"(%s) write %s/unknown size, %s/s",
					logPrefix,
					humanize.Bytes(uint64(p.N())),
					humanize.Bytes(uint64(speed)),
				)
			}
		}
		log.Infof("(%s) write streaming completed", logPrefix)
	}()
	return progressWriter
}

// Create a reader with progress
func NewProgressReader(ctx context.Context, r io.Reader, logPrefix string, totalSize int64, timer *Timer, interval time.Duration) io.Reader {
	progressReader := progress.NewReader(r)
	// Start a goroutine printing progress
	go func() {
		progressChan := progress.NewTicker(ctx, progressReader, totalSize, interval)
		for p := range progressChan {
			speed := float64(p.N()) / timer.ElapsedSeconds()
			if totalSize >= 0 {
				log.Infof(
					"(%s) read %s/%s, %.2f pct, %s/s, %v remaining...",
					logPrefix,
					humanize.Bytes(uint64(p.N())),
					humanize.Bytes(uint64(p.Size())),
					p.Percent(),
					humanize.Bytes(uint64(speed)),
					p.Remaining().Round(time.Second),
				)
			} else {
				log.Infof(
					"(%s) read %s/unknown size, %s/s",
					logPrefix,
					humanize.Bytes(uint64(p.N())),
					humanize.Bytes(uint64(speed)),
				)
			}
		}
		log.Infof("(%s) read streaming completed", logPrefix)
	}()
	return progressReader
}
