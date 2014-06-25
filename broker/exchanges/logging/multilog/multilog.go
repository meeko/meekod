// Copyright (c) 2013 The meeko AUTHORS
//
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package multilog

import "github.com/meeko/meekod/broker/services/logging"

//------------------------------------------------------------------------------
// MultiLogger implements logging.Exchange
//------------------------------------------------------------------------------

// MultiLogger works much like the UNIX tea command for logging.Exchange.
// It wraps multiple logging.Exchange instances and distributes the method calls to all of them.
type MultiLogger struct {
	exchanges []logging.Exchange
}

func New(exchange ...logging.Exchange) *MultiLogger {
	return
}

func (log *MultiLogger) Log(agent string, level logging.Level, message []byte) error {
	for _, ex := range log.exchanges {
		if err := ex.Log(agent, level, message); err != nil {
			return err
		}
	}
	return nil
}

func (log *MultiLogger) Flush() {
	for _, ex := range log.exchanges {
		ex.Flush()
	}
}

func (log *MultiLogger) Close() {
	for _, ex := range log.exchanges {
		ex.Close()
	}
}
