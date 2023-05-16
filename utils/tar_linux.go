package utils

import (
	"archive/tar"
	"fmt"
	"io/fs"
	"syscall"
)

func handleHardlink(info fs.FileInfo, path string, header *tar.Header, hardlinkMap map[string]*hardlink) error {
	t, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("invalid info.Sys of linux")
	}
	if t.Nlink > 1 {
		key := fmt.Sprintf("%d.%d", t.Ino, t.Dev)
		hard, ok := hardlinkMap[key]
		if !ok {
			hard = new(hardlink)
			hard.count = uint64(t.Nlink)
			hard.link = path
			hardlinkMap[key] = hard
		} else {
			header.Typeflag = tar.TypeLink
			header.Linkname = hard.link
			header.Size = 0
			hard.count--
			if hard.count == 0 {
				delete(hardlinkMap, key)
			}
		}
	}
	return nil
}
