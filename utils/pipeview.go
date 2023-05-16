package utils

import (
	"fmt"
	"os"
	"os/exec"
)

// Pipeview to package or unpackage files
type Pipeview struct {
	Main    string `json:"main,omitempty"`
	LogFile string `json:"log_file,omitempty"`
	Limit   string `json:"limit,omitempty"`
	Output  string `json:"output,omitempty"`
	Input   string `json:"-"`
}

// GenerateCMD generate xtrabackup command from args
func (p *Pipeview) GenerateCMD() (cmd *exec.Cmd, err error) {
	// init cmdStr
	var cmdStr string

	// check pv command existence
	if _, err := os.Stat(p.Main); os.IsNotExist(err) {
		return nil, err
	}

	// set default value for empty outputFile
	if p.Output == "" {
		p.Output = "stdout"
	}

	// pv output to file
	if p.Output != "stdout" {
		if _, err := os.Stat(p.Output); err == nil {
			return nil, fmt.Errorf("target file(%s) exists", p.Output)
		}
		cmdStr = fmt.Sprintf(
			"%s -nbtf -L%s %s 1>%s 2>>%s",
			p.Main,
			p.Limit,
			p.Input,
			p.Output,
			p.LogFile,
		)
	} else {
		// get pv command
		cmdStr = fmt.Sprintf(
			"%s -nbtf -L%s %s 2>>%s",
			p.Main,
			p.Limit,
			p.Input,
			p.LogFile,
		)
	}

	cmd = exec.Command("sh", "-c", cmdStr)

	return cmd, nil
}
