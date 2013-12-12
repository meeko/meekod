// Copyright (c) 2013 The cider AUTHORS
//
// Use of this source code is governed by The MIT License
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

	// Cider
	"github.com/cider/cider/broker"
	"github.com/cider/cider/broker/exchanges/logging/seelog"
	"github.com/cider/cider/broker/exchanges/pubsub/eventbus"
	"github.com/cider/cider/broker/exchanges/rpc/roundrobin"
	ciderLog "github.com/cider/cider/broker/log"
	zlogging "github.com/cider/cider/broker/transports/zmq3/logging"
	zpubsub "github.com/cider/cider/broker/transports/zmq3/pubsub"
	zrpc "github.com/cider/cider/broker/transports/zmq3/rpc"

	// Others
	_ "github.com/joho/godotenv/autoload" // Load .env on init.
	zmq "github.com/pebbe/zmq3"
)

func main() {
	// Get rid of the date and time log prefix.
	log.SetFlags(0)

	// Process command line.
	enableZmq3RPC := flag.Bool("enable_rpc_zmq3", true, "enable RPC service over ZeromMQ 3.x")
	enableZmq3PubSub := flag.Bool("enable_pubsub_zmq3", true, "enable PubSub service over ZeromMQ 3.x")
	enableZmq3Logging := flag.Bool("enable_logging_zmq3", true, "enable Logging service over ZeromMQ 3.x")
	verbose := flag.Bool("v", false, "enable logging to stderr")
	help := flag.Bool("h", false, "print help and exit")

	flag.Parse()

	if *help {
		Usage()
	}

	if *verbose {
		// Let the broker use the same logger as the seelog Logging exchange.
		ciderLog.UseLogger(seelog.Default)
	}

	// Start catching signals.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Instantiate exchanges. Every service (RPC, PubSub, Logging ...) has its
	// own exchange, which is basically the common point where messages are
	// exchanged between various transport endpoints of the same service.
	//
	// As an example, eventBus is the exchange for PubSub and the ZeroMQ 3.x
	// transport is plugged into it. If someone writer let's say a WebSocket
	// transport endpoint, it then gets plugged into the same exchange and all
	// events emitted on that transport will get broadcasted to all the transports
	// connected to the same exchange.
	var (
		balancer = roundrobin.NewBalancer()
		eventBus = eventbus.New()
		logger   = seelog.Default
	)
	defer logger.Close()

	// ZeroMQ 3.x endpoints
	if *enableZmq3RPC {
		log.Println("Configuring ZeroMQ 3.x endpoint for RPC ...")
		broker.RegisterEndpointFactory("zmq3_rpc", func() (broker.Endpoint, error) {
			config := zrpc.NewEndpointConfig()
			config.MustFeedFromEnv("CIDER_ZMQ3_RPC_").MustBeComplete()
			return zrpc.NewEndpoint(config, balancer)
		})
	}

	if *enableZmq3PubSub {
		log.Println("Configuring ZeroMQ 3.x endpoint for PubSub ...")
		broker.RegisterEndpointFactory("zmq3_pubsub", func() (broker.Endpoint, error) {
			config := zpubsub.NewEndpointConfig()
			config.MustFeedFromEnv("CIDER_ZMQ3_PUBSUB_").MustBeComplete()
			return zpubsub.NewEndpoint(config, eventBus)
		})
	}

	if *enableZmq3Logging {
		log.Println("Configuring ZeroMQ 3.x endpoint for Logging ...")
		broker.RegisterEndpointFactory("zmq3_logging", func() (broker.Endpoint, error) {
			config := zlogging.NewEndpointConfig()
			config.MustFeedFromEnv("CIDER_ZMQ3_LOGGING_").MustBeComplete()
			return zlogging.NewEndpoint(config, logger)
		})
	}

	// Start processing signals.
	go func() {
		<-signalCh
		log.Println("Signal received, terminating ...")
		broker.Terminate()
	}()

	// Register a monitoring channel.
	monitorCh := make(chan *broker.EndpointCrashReport)
	broker.Monitor(monitorCh)

	// ListenAndServe returns a channel that can be used for monitoring running
	// endpoints. An error is sent there every time an endpoint crashes.
	// If there is no endpoint left or broker is simply terminated, the channel
	// is closed.
	log.Println("Broker configured, starting requested endpoints ...")
	broker.ListenAndServe()
	for err := range monitorCh {
		log.Printf("Endpoint %v crashed with error=%v\n", err.FactoryId, err.Error)
		if err.Dropped {
			log.Printf("Endpoint %v dropped", err.FactoryId)
		}
	}

	log.Println("Waiting for pending messages to be sent ...")
	zmq.Term()
	log.Println("Broker terminated")
}

func Usage() {
	fmt.Fprint(os.Stderr, `APPLICATION
  cider-broker - Cider broker standalone executable

VERSION
`)
	fmt.Fprintf(os.Stderr, "  %s\n", broker.Version)
	fmt.Fprint(os.Stderr, `
USAGE
  cider-broker [ OPTION ... ]

OPTIONS
`)
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, `
DESCRIPTION
  cider-broker starts the Cider broker component as a standalone process.

`)
	os.Exit(2)
}
