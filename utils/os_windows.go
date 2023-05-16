package utils

import (
	"fmt"
)

func getPidByListenPortAndCMD(port int, cmd string) (pid int, err error) {
	return -1, fmt.Errorf("not support windows yet")
}
