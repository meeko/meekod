// Copyright (c) 2013 The meeko AUTHORS
//
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package main

import (
	// Stdlib
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	// Meeko
	"github.com/meeko/meekod/broker"
	broklog "github.com/meeko/meekod/broker/log"
	"github.com/meeko/meekod/daemon"

	// Other
	"github.com/cihub/seelog"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln("\nError:", err)
	}
}

func run() error {
	// Get rid of the date and time log prefix.
	log.SetFlags(0)

	// Parse configuration.
	var (
		configPath   = os.Getenv("MEEKOD_CONFIG")
		noSupervisor = os.Getenv("MEEKOD_DISABLE_SUPERVISOR") != ""
		noIPC        = os.Getenv("MEEKOD_DISABLE_PLATFORM_ENDPOINTS") != ""
		verbose      = os.Getenv("MEEKOD_VERBOSE") != ""
	)
	flag.StringVar(&configPath, "config", configPath, "configuration file path")
	flag.BoolVar(&noSupervisor, "disable_supervisor", noSupervisor, "disable the agent supervisor")
	flag.BoolVar(&noIPC, "disable_ipc", noIPC, "disable all IPC service endpoints")
	flag.BoolVar(&verbose, "verbose", verbose, "enable more verbose logging")
	help := flag.Bool("h", false, "print help and exit")
	flag.Parse()

	if *help {
		Usage()
	}

	// TODO: Logging is a mess, it need some more thinking.
	if verbose {
		broklog.UseLogger(seelog.Default)
		defer broklog.Flush()
	} else {
		seelog.ReplaceLogger(seelog.Disabled)
		defer seelog.Flush()
	}

	// Create a daemon instance from the specified config file.
	dmn, err := daemon.NewFromConfigAsFile(configPath, &daemon.Options{
		DisableSupervisor:     noSupervisor,
		DisableLocalEndpoints: noIPC,
	})
	if err != nil {
		return err
	}
	defer func() {
		dmn.Terminate()
		log.Println("The background thread terminated")
	}()

	// Start catching signals.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Register a monitoring channel.
	monitorCh := make(chan *broker.EndpointCrashReport)
	dmn.Monitor(monitorCh)

	// Start the daemon.
	log.Println("Starting the background thread")
	dmnErrorCh := make(chan error, 1)
	go func() {
		dmnErrorCh <- dmn.Serve()
	}()

	// Loop until interrupted.
	for {
		select {
		case report, ok := <-monitorCh:
			if !ok {
				continue
			}
			if report.Dropped {
				return fmt.Errorf("Endpoint %v dropped", report.FactoryId)
			}
			log.Printf("Endpoint %v crashed: %v\n", report.FactoryId, report.Error)

		case err := <-dmnErrorCh:
			return err

		case <-signalCh:
			log.Println("Signal received, terminating...")
			return nil
		}
	}

	return nil
}

func Usage() {
	fmt.Fprint(os.Stderr, `APPLICATION:
  meekod - Meeko daemon

VERSION:
`)
	fmt.Fprintf(os.Stderr, "  %s\n", broker.Version)
	fmt.Fprint(os.Stderr, `
USAGE:
  meeko [ OPTION ... ]

OPTIONS:
`)
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, `
DESCRIPTION:
  This command starts the Meeko daemon, which incorporated the broker component
  and the agent supervisor component.

ENVIRONMENT:
  MEEKOD_CONFIG
  MEEKOD_DISABLE_SUPERVISOR
  MEEKOD_DISABLE_LOCAL_ENDPOINTS
  MEEKOD_VERBOSE

  The meaning of these environment variables is the same as their flag
  counterparts. A variable is evaluated as true when it is set.

`)
	os.Exit(2)
}
