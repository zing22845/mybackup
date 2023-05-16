package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hako/durafmt"
	log "github.com/sirupsen/logrus"
	"k8s.io/utils/strings/slices"
)

func DIRCount(path string, excludeDIRs []string) (count int64, size int64, err error) {
	if len(excludeDIRs) > 0 {
		log.Infof("count will skip: %s", strings.Join(excludeDIRs, ", "))
	}
	err = filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		// check exclude dirs
		if slices.Contains(excludeDIRs, p) {
			return filepath.SkipDir
		}
		count++
		if info != nil && info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	})
	return count, size, err
}

func RemoveEmptyDIR(path string) (err error) {
	if path == "" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("get stat of %s failed: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	err = os.Remove(path)
	return err
}

func CreateDIR(path string) (err error) {
	if _, err = os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		err = os.MkdirAll(path, 0755)
	}
	return err
}

func GetFileContentType(filePath string) (string, error) {

	// Open File
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Only the first 512 bytes are used to sniff the content type.
	buffer := make([]byte, 512)

	_, err = f.Read(buffer)
	if err != nil && err != io.EOF {
		return "", err
	}

	// Use the net/http package's handy DectectContentType function. Always returns a valid
	// content-type by returning "application/octet-stream" if no others seemed to match.
	contentType := http.DetectContentType(buffer)

	return contentType, nil
}

// get uncompressed size of a gzip compressed file by last 4 bytes
func GetGzipUncompressedSize(filePath string) (uncompressedSize uint32, err error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 4)
	_, err = f.ReadAt(buf, info.Size()-4)
	if err != nil {
		return 0, fmt.Errorf("read last 4 bytes of file(%s) faild: %w", filePath, err)
	}

	uncompressedSize = binary.LittleEndian.Uint32(buf)
	return uncompressedSize, nil
}

// clean expired files in folder
func CleanExpiredSubFiles(filePath string, expireDuration time.Duration) (err error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return nil
	}

	expireDatetime := time.Now().Add(-expireDuration)
	log.Infof("deleting files in directory %s last modify time before %s, expireDuration: %s",
		filePath, expireDatetime.Format(time.RFC3339), durafmt.Parse(expireDuration))
	return filepath.Walk(filePath, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Stat(file)
		if err != nil || info.ModTime().After(expireDatetime) {
			return nil
		}
		warn := os.Remove(file)
		if warn != nil {
			log.Infof("failed to remove expired file: %s, ModTime: %s, err: %s", file, info.ModTime(), warn)
			return nil
		}
		log.Infof("removed expired file: %s, ModTime: %s", file, info.ModTime())
		return nil
	})
}

func GetPidByListenPortAndCMD(port int, cmd string) (pid int, err error) {
	return getPidByListenPortAndCMD(port, cmd)
}
