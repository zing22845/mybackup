package utils

import (
	"fmt"
	"io"
	"os"
	"os/exec"
)

// Xbstream to package or unpackage files
type Xbstream struct {
	Main              string    `json:"main,omitempty"`
	Action            string    `json:"action,omitempty"`
	Parallel          uint      `json:"parallel,omitempty"`
	DecompressSwitch  uint      `json:"decompress_switch,omitempty"`
	DecompressThreads uint      `json:"decompress_threads,omitempty"`
	DecryptMethod     string    `json:"decrypt_method,omitempty"`
	EncryptKey        string    `json:"encrypt_key,omitempty"`
	Directory         string    `json:"directory,omitempty"`
	CMD               *exec.Cmd `json:"-"`
	LogWriter         io.Writer `json:"-"`
}

// GenerateCMD generate extract command from args
func (x *Xbstream) GenerateCMD() (err error) {
	if x.Action == "extract" {
		return x.generateExtractCMD()
	} else if x.Action == "create" {
		return x.generateCreateCMD()
	}
	return fmt.Errorf("unsupport xbstream action: %s", x.Action)
}

// GenerateExtractCMD generate xbstream extract command
func (x *Xbstream) generateExtractCMD() (err error) {
	// set --parallel for xbstream
	xbstreamOptParallel := ""
	if x.Parallel > 0 {
		xbstreamOptParallel = fmt.Sprintf("--parallel=%d", x.Parallel)
	}
	// set xbstream action
	xbstreamOptAction := "--extract"
	// set --decompress and --decompress-threads
	xbstreamOptDecompress := ""
	if x.DecompressSwitch == 1 {
		xbstreamOptDecompress = fmt.Sprintf(
			"--decompress --decompress-threads=%d",
			x.DecompressThreads,
		)
	}
	// set --decrypt and encrypt-key
	xbstreamOptDecrypt := ""
	if x.DecryptMethod == "" {
		xbstreamOptDecrypt = ""
	} else if !Contains(SupportEncryptMethod, x.DecryptMethod) {
		return fmt.Errorf(
			"unsupport encrypt method: %s",
			x.DecryptMethod,
		)
	} else if len(x.EncryptKey) != 32 {
		return fmt.Errorf(
			"invalid encrypt key length: %d",
			len(x.EncryptKey),
		)
	} else {
		xbstreamOptDecrypt = fmt.Sprintf(
			"--decrypt=%s --encrypt-key=%s",
			x.DecryptMethod,
			x.EncryptKey,
		)
	}
	// set --directory
	if x.Directory == "" {
		return fmt.Errorf("no target dir, invalid backup command")
	}
	if _, err := os.Stat(x.Directory); err == nil {
		return fmt.Errorf("target directory(%s) exists", x.Directory)
	}
	err = CreateDIR(x.Directory)
	if err != nil {
		return fmt.Errorf("create directory(%s) failed", x.Directory)
	}
	xbstreamOptDirectory := fmt.Sprintf("--directory=%s", x.Directory)
	// get xbstream command
	cmdStr := fmt.Sprintf("%s -v %s %s %s %s %s",
		x.Main,
		xbstreamOptAction,
		xbstreamOptParallel,
		xbstreamOptDecompress,
		xbstreamOptDecrypt,
		xbstreamOptDirectory,
	)
	x.CMD = exec.Command("sh", "-c", cmdStr)
	x.CMD.Stderr = x.LogWriter
	return nil
}

// GenerateCreateCMD generate xbstream create command
func (x *Xbstream) generateCreateCMD() (err error) {
	// set --parallel for xbstream
	xbstreamOptParallel := ""
	if x.Parallel > 0 {
		xbstreamOptParallel = fmt.Sprintf("--parallel=%d", x.Parallel)
	}
	// set xbstream action
	xbstreamOptAction := "--create"
	// set --directory
	if x.Directory == "" {
		return fmt.Errorf("no backup target dir, invalid backup command")
	}
	xbstreamOptDirectory := fmt.Sprintf("--directory=%s", x.Directory)
	// get xbstream command
	cmdStr := fmt.Sprintf("%s -v %s %s %s",
		x.Main,
		xbstreamOptAction,
		xbstreamOptParallel,
		xbstreamOptDirectory,
	)
	x.CMD = exec.Command("sh", "-c", cmdStr)
	x.CMD.Stderr = x.LogWriter
	return nil
}
