package utils

import (
	"fmt"
	"os"
	"os/exec"
)

// pigz to parallel compress or uncompress files
type Pigz struct {
	Main    string `json:"main,omitempty"`
	LogFile string `json:"log_file,omitempty"`
}

func (p *Pigz) GenerateCompressCMD() (cmd *exec.Cmd, err error) {
	// check pigz command existence
	if _, err := os.Stat(p.Main); os.IsNotExist(err) {
		return nil, err
	}

	// get pigz command
	cmdStr := fmt.Sprintf("%s -c 2>>%s", p.Main, p.LogFile)

	cmd = exec.Command("sh", "-c", cmdStr)

	return cmd, nil
}

// GenerateCMD generate pigz command from args
func (p *Pigz) GenerateDecompressCMD() (cmd *exec.Cmd, err error) {
	// check pigz command existence
	if _, err := os.Stat(p.Main); err != nil {
		return nil, err
	}

	// get pigz command
	cmdStr := fmt.Sprintf("%s -dc 2>>%s", p.Main, p.LogFile)

	cmd = exec.Command("sh", "-c", cmdStr)

	return cmd, nil
}
