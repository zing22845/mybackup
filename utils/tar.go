package utils

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"k8s.io/utils/strings/slices"
)

var regMissingChars = regexp.MustCompile(`^archive/tar: missed writing (\d+) bytes$`)

type hardlink struct {
	link  string
	count uint64
}

// Tar specified file path to writer
// nolint:gocyclo
func Tar(filePath, arcname string, excludeDIRs []string, ignoreFileChange, ignoreAllErrors bool, w io.Writer) (size int64, err error) {
	tarWriter := tar.NewWriter(w)
	defer tarWriter.Close()
	if len(excludeDIRs) > 0 {
		log.Infof("archive/tar will skip dirs: %s", strings.Join(excludeDIRs, ", "))
	}
	hardlinkMap := make(map[string]*hardlink)
	// walk through every file in the folder
	err = filepath.Walk(filePath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				if errors.Is(err, io.ErrClosedPipe) {
					return fmt.Errorf("(%s) walk failed: %w", path, err)
				}
				if !ignoreAllErrors {
					return fmt.Errorf("(%s) walk failed: %w", path, err)
				}
				log.Warnf("(%s) ignored_fatal_error: %s", path, err)
				return nil
			}
			// chek exclude dirs
			if slices.Contains(excludeDIRs, path) {
				return filepath.SkipDir
			}
			if info == nil {
				log.Warnf("(%s) get file info failed", path)
			}
			soft := ""
			// softlink
			if info.Mode()&fs.ModeSymlink != 0 {
				soft, err = os.Readlink(path)
				if err != nil {
					if !ignoreAllErrors {
						return fmt.Errorf("(%s) read link failed: %w", path, err)
					}
					log.Warnf("(%s) ignored_fatal_error: %s", path, err)
					return nil
				}
			}
			// generate tar header
			header, err := tar.FileInfoHeader(info, soft)
			if err != nil {
				if strings.Contains(err.Error(), "archive/tar: ") {
					log.Warnf("(%s) warn: %s", path, err)
					return nil
				}
				if !ignoreAllErrors {
					return fmt.Errorf("get header of %s failed: %w", path, err)
				}
				log.Warnf("(%s) ignored_fatal_error: %s", path, err)
				return nil
			}
			// deal hardlink
			if info.Mode().IsRegular() {
				err = handleHardlink(info, path, header, hardlinkMap)
				if err != nil {
					if !ignoreAllErrors {
						return fmt.Errorf("(%s) process hardlink failed: %w", path, err)
					}
					log.Warnf("(%s) ignored_fatal_error: %s", path, err)
					return nil
				}
			}
			// must provide real name
			// (see https://golang.org/src/archive/tar/common.go?#L626)
			if arcname != "" {
				relativePath, err := filepath.Rel(filePath, path)
				if err != nil {
					if !ignoreAllErrors {
						return fmt.Errorf("(%s) get relative path of base %s failed: %w",
							path, filePath, err)
					}
					log.Warnf("(%s) ignored_fatal_error: %s", path, err)
					return nil
				}
				header.Name = filepath.ToSlash(filepath.Join(arcname, relativePath))
			} else {
				header.Name = filepath.ToSlash(path)
			}
			// write header
			if err = tarWriter.WriteHeader(header); err != nil {
				if errors.Is(err, io.ErrClosedPipe) {
					return fmt.Errorf("(%s) write header failed: %w", path, err)
				}
				if !ignoreAllErrors {
					return fmt.Errorf("write header of %s failed: %w", path, err)
				}
				log.Warnf("(%s) ignored_fatal_error: %s", path, err)
				return nil
			}
			// skip write header only file
			if isHeaderOnlyType(header.Typeflag) || header.Size <= 0 {
				return nil
			}
			// open files for taring
			f, err := os.Open(path)
			if err != nil {
				if !ignoreAllErrors {
					return fmt.Errorf("(%s) open file failed: %w", path, err)
				}
				log.Warnf("(%s) ignored_fatal_error: %s", path, err)
				return nil
			}
			defer f.Close()
			// copy file data into tar writer
			n, err := io.Copy(tarWriter, f)
			if err != nil {
				if ignoreFileChange && errors.Is(err, tar.ErrWriteTooLong) {
					log.Warnf("(%s) ignore file change with warn: %s", path, err)
				} else if ignoreAllErrors {
					log.Warnf("(%s) ignored_fatal_error: %s", path, err)
				} else {
					return fmt.Errorf("(%s) type(%s), copy to tar writer failed: %w", path, string(header.Typeflag), err)
				}
			}
			size = size + n
			if err = tarWriter.Flush(); err != nil {
				subMatch := regMissingChars.FindStringSubmatch(err.Error())
				if len(subMatch) == 0 {
					if !ignoreAllErrors {
						return fmt.Errorf("flush %s failed: %w", path, err)
					}
					log.Warnf("(%s) ignored_fatal_error: %s", path, err)
					return nil
				}
				log.Infof("flush %s err: %v, padding with zeros", path, err)
				zeroPadCount, err := strconv.Atoi(subMatch[1])
				if err != nil {
					if !ignoreAllErrors {
						return fmt.Errorf("(%s) convert error size %s failed: %w", path, subMatch[1], err)
					}
					log.Warnf("(%s) ignored_fatal_error: %s", path, err)
					return nil
				}
				m, err := tarWriter.Write(make([]byte, zeroPadCount))
				if err != nil {
					if !ignoreAllErrors {
						return fmt.Errorf("(%s) write %d zero pad failed: %w", path, zeroPadCount, err)
					}
					log.Warnf("(%s) ignored_fatal_error: %s", path, err)
					return nil
				}
				size = size + int64(m)
			}
			return nil
		})

	if err != nil {
		return 0, err
	}
	// produce tar
	if err = tarWriter.Close(); err != nil {
		return 0, err
	}
	return size, nil
}

// Untar the reader to specified file path
func Untar(r io.Reader, filePath string) (size int64, err error) {
	err = CreateDIR(filePath)
	if err != nil {
		return 0, err
	}
	tr := tar.NewReader(r)
	for {
		header, err := tr.Next()
		switch {
		// if no more files are found return
		case errors.Is(err, io.EOF):
			log.Infof("readed %d bytes before EOF", size)
			// discard bytes after EOF prevent blocking pipe writer
			// there are padded zeros after EOF, if the zeros count is grater than default pipe buffer(4096), the pipe wirter will blocked until all zeros written
			n, err := io.Copy(io.Discard, r)
			if err != nil {
				log.Warnf("discard zeros with err: %+v", err)
			}
			log.Infof("dicarded %d bytes after EOF", n)
			return size, nil

		// return any other error
		case err != nil:
			return size, fmt.Errorf("tr.Next failed: %w", err)

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			log.Warnf("empty header")
			continue
		}
		// the target location where the dir/file should be created
		target := filepath.Join(filePath, header.Name)

		// the following switch could also be done using fi.Mode(), not sure if there
		// a benefit of using one vs. the other.
		// fi := header.FileInfo()

		// check the file type
		switch header.Typeflag {
		// if its a dir and it doesn't exist create it
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return size, err
				}
			}
		// if it's a file create it
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return size, err
			}

			// copy over contents
			n, err := io.Copy(f, tr)
			if err != nil {
				return size, fmt.Errorf("io.Copy failed: %w", err)
			}
			size += n

			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			err = f.Close()
			if err != nil {
				return size, err
			}
		case tar.TypeLink:
			linkTarget := filepath.Join(filePath, header.Linkname)
			err = os.Link(linkTarget, target)
			if err != nil {
				return size, fmt.Errorf("(%s) hardlink to %s failed: %w", target, header.Linkname, err)
			}
		case tar.TypeSymlink:
			err = os.Symlink(header.Linkname, target)
			if err != nil {
				return size, fmt.Errorf("(%s) symlink to %s failed: %w", target, header.Linkname, err)
			}
		}
	}
}

func isHeaderOnlyType(flag byte) bool {
	switch flag {
	case tar.TypeLink, tar.TypeSymlink, tar.TypeChar, tar.TypeBlock, tar.TypeDir, tar.TypeFifo:
		return true
	default:
		return false
	}
}
