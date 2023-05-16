package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bastjan/netstat"
)

func getPidByListenPortAndCMD(port int, cmd string) (pid int, err error) {
	pidMapCmd := make(map[int]string)
	pids := make([]int, 0)

	// check tcpv4 listener processes
	if _, err := os.Stat("/proc/net/tcp"); err == nil {
		tcp4Connections, err := netstat.TCP.Connections()
		if err != nil {
			return -1, fmt.Errorf("read tcp4 proc file failed: %w", err)
		}
		for _, conn := range tcp4Connections {
			if conn.Port == port && conn.State == netstat.TCPListen {
				if conn.Exe == "" {
					// no permission to read /proc/${pid}/exec
					continue
				}
				if cmd == "" || filepath.Base(conn.Exe) == cmd {
					if _, ok := pidMapCmd[conn.Pid]; !ok {
						pids = append(pids, conn.Pid)
					}
					pidMapCmd[conn.Pid] = conn.Exe
				}
			}
			if len(pidMapCmd) > 1 {
				return -1, fmt.Errorf("find multiple process listening on port %d, pids: %s",
					port, strings.Trim(strings.Replace(fmt.Sprint(pids), " ", ",", -1), "[]"))
			}
		}
	}

	// check tcpv6 listener processes
	if _, err := os.Stat("/proc/net/tcp6"); err == nil {
		tcp6Connections, err := netstat.TCP6.Connections()
		if err != nil {
			return -1, fmt.Errorf("read tcp6 proc file failed: %w", err)
		}
		for _, conn := range tcp6Connections {
			if conn.Port == port && conn.State == netstat.TCPListen {
				if conn.Exe == "" {
					// no permission to read /proc/${pid}/exec
					continue
				}
				if cmd == "" || filepath.Base(conn.Exe) == cmd {
					if _, ok := pidMapCmd[conn.Pid]; !ok {
						pids = append(pids, conn.Pid)
					}
					pidMapCmd[conn.Pid] = conn.Exe
				}
			}
			if len(pidMapCmd) > 1 {
				return -1, fmt.Errorf("find multiple process listening on port %d, pids: %s",
					port, strings.Trim(strings.Replace(fmt.Sprint(pids), " ", ",", -1), "[]"))
			}
		}
	}

	if len(pids) == 0 {
		return -1, fmt.Errorf("no permitted process(%s) listening on port %d was found", cmd, port)
	}

	return pids[0], nil
}
