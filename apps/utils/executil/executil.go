// Copyright (c) 2013 The cider AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package executil

import (
	"os"
	"os/exec"
	//"syscall"
	"time"
)

func Run(cmd *exec.Cmd, interrupted <-chan struct{}) error {
	//var attr syscall.SysProcAttr
	//attr.Noctty = true
	//cmd.SysProcAttr = &attr

	if err := cmd.Start(); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- cmd.Wait()
	}()

	select {
	case err := <-errCh:
		return err
	case <-interrupted:
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			return err
		}

		select {
		case err := <-errCh:
			return err
		case <-time.After(3 * time.Second):
			if err := cmd.Process.Signal(os.Kill); err != nil {
				return err
			}

			return <-errCh
		}
	}

	return nil
}
