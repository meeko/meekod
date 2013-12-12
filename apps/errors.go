// Copyright (c) 2013 The cider AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package apps

import "errors"

var (
	ErrUnknownAlias  = errors.New("unknown app alias")
	ErrAppRunning    = errors.New("app is running")
	ErrAppNotRunning = errors.New("app is not running")
	ErrInterrupted   = errors.New("operation was interrupted")
)
