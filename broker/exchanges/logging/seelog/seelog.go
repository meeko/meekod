// Copyright (c) 2013 The meeko AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package seelog

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/meeko/meekod/broker/services/logging"

	"github.com/cihub/seelog"
)

//------------------------------------------------------------------------------
// Logger implements logging.Handler
//------------------------------------------------------------------------------

type FileLogger struct {
	dir     string
	loggers map[string]seelog.LoggerInterface
	mu      *sync.Mutex
}

func NewFileLogger(directory string) (*FileLogger, error) {
	// Make sure that directory is a directory.
	info, err := os.Lstat(directory)
	if err != nil {
		if os.IsNotExist(err) {
			if err := createDir(directory); err != nil {
				return nil, err
			}
		}
		return nil, err
	}
	if !info.IsDir() {
		return nil, &ErrNotDirectory{directory}
	}

	// Make sure that it is writable.
	tmp, err := ioutil.TempFile(directory, "cider")
	if err != nil {
		return nil, err
	}
	// os.Remove(tmp.Name()) might be enough here...
	os.Remove(filepath.Join(directory, filepath.Base(tmp.Name())))
	tmp.Close()

	return &FileLogger{
		dir:     directory,
		loggers: make(map[string]seelog.LoggerInterface),
		mu:      new(sync.Mutex),
	}, nil
}

func (logger *FileLogger) Log(agent string, level logging.Level, message []byte) error {
	log := logger.getOrCreateLogger(agent)
	msg := string(message)

	switch level {
	case logging.LevelTrace:
		log.Trace(msg)
	case logging.LevelDebug:
		log.Debug(msg)
	case logging.LevelInfo:
		log.Info(msg)
	case logging.LevelWarn:
		log.Warn(msg)
	case logging.LevelCritical:
		log.Critical(msg)
	}

	return nil
}

func (logger *FileLogger) Flush() {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	for _, log := range logger.loggers {
		log.Flush()
	}
}

func (logger *FileLogger) Close() {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	for agent, log := range logger.loggers {
		log.Close()
		defer func(agt string) {
			delete(logger.loggers, agt)
		}(agent)
	}
}

func (logger *FileLogger) getOrCreateLogger(agent string) seelog.LoggerInterface {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	log, ok := logger.loggers[agent]
	if !ok {
		var config bytes.Buffer
		io.WriteString(&config, `
<seelog>
  <outputs format="debug-short">
		`)
		fmt.Fprintf(&config, "<file path=\"%v\">", filepath.Join(logger.dir, agent+".log"))
		io.WriteString(&config, `
  </outputs>
</seelog>
		`)

		log, err := seelog.LoggerFromConfigAsBytes(config.Bytes())
		if err != nil {
			panic(err)
		}
		logger.loggers[agent] = log
	}
	return log
}

// Errors ----------------------------------------------------------------------

type ErrNotDirectory struct {
	path string
}

func (err *ErrNotDirectory) Error() string {
	return fmt.Sprintf("not a directory: %v", err.path)
}
