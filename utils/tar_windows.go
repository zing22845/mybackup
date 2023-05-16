package utils

import (
	"archive/tar"
	"io/fs"
)

func handleHardlink(info fs.FileInfo, path string, header *tar.Header, hardlinkMap map[string]*hardlink) error {
	return nil
}
