package utils

import (
	"fmt"
	"io"
	"os/exec"
	"syscall"
)

// ShellCMDS shell cmds chained by pipline
type ShellCMDS []*exec.Cmd

// Pipe combine commands with pipeline
func (scs *ShellCMDS) Pipe() (pipeOut io.ReadCloser, pipeIn io.WriteCloser, err error) {
	last := len(*scs) - 1
	for i, cmd := range *scs {
		// pipe pipeIn writer to first stdin
		if i == 0 {
			if pipeIn, err = cmd.StdinPipe(); err != nil {
				return nil, nil, fmt.Errorf(
					"Pipe in to command '%s' failed: %w",
					cmd.String(), err)
			}
		}
		// pipe commands
		if i != last {
			if (*scs)[i+1].Stdin, err = cmd.StdoutPipe(); err != nil {
				return nil, nil, fmt.Errorf(
					"Pipe command '%s' to '%s' failed: %w",
					cmd.String(), (*scs)[i+1].String(), err)
			}
			continue
		}
		// pipe last stdout to pipeOut
		if pipeOut, err = cmd.StdoutPipe(); err != nil {
			return nil, nil, fmt.Errorf("Pipe out from command '%s' failed: %w",
				cmd.String(), err)
		}
	}
	return pipeOut, pipeIn, err
}

// Start start commands
func (scs *ShellCMDS) Start() (err error) {
	// Start each command
	for _, cmd := range *scs {
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start command '%s' failed: %w",
				cmd.String(), err)
		}
	}
	return nil
}

// Wait wait commands to finish
func (scs *ShellCMDS) Wait() error {
	// Wait for each command to complete
	for i := len(*scs) - 1; i >= 0; i-- {
		cmd := (*scs)[i]
		err := cmd.Wait()
		status := cmd.ProcessState.Sys().(syscall.WaitStatus)
		exitStatus := status.ExitStatus()
		signaled := status.Signaled()
		signal := status.Signal()
		if err != nil {
			return fmt.Errorf(
				"wait cmd failed, cmd: %s, status: %d, signaled: %v, signal: %d, error: %w",
				cmd.String(), exitStatus, signaled, signal, err)
		}
	}
	return nil
}
