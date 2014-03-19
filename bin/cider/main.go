// Copyright (c) 2013 The cider AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package main

import (
	// Stdlib
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Cider broker
	"github.com/cider/cider/broker"
	"github.com/cider/cider/broker/exchanges/logging/publisher"
	"github.com/cider/cider/broker/exchanges/pubsub/eventbus"
	"github.com/cider/cider/broker/exchanges/rpc/roundrobin"
	ciderLog "github.com/cider/cider/broker/log"
	wsrpc "github.com/cider/cider/broker/transports/websocket/rpc"
	zlogging "github.com/cider/cider/broker/transports/zmq3/logging"
	zpubsub "github.com/cider/cider/broker/transports/zmq3/pubsub"
	zrpc "github.com/cider/cider/broker/transports/zmq3/rpc"

	// Cider client
	rpc_client "github.com/cider/go-cider/cider/services/rpc"
	rpc_inproc "github.com/cider/go-cider/cider/transports/inproc/rpc"

	// Cider apps
	"github.com/cider/cider/apps"
	"github.com/cider/cider/apps/supervisors/exec"

	// Others
	ws "code.google.com/p/go.net/websocket"
	"github.com/cihub/seelog"
	_ "github.com/joho/godotenv/autoload" // Load .env on init.
	zmq "github.com/pebbe/zmq3"
)

const AppsKillTimeout = 5 * time.Second

func main() {
	if err := _main(); err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func _main() error {
	// Get rid of the date and time log prefix.
	log.SetFlags(0)

	// Process command line.
	workspace := flag.String("workspace", "", "directory to use as the workspace")
	dbURL := flag.String("mgo_url", "localhost", "mgo-compatible MongoDB URL")
	enableZmq3RPC := flag.Bool("enable_rpc_zmq3", true, "enable RPC service over ZeromMQ 3.x")
	enableZmq3PubSub := flag.Bool("enable_pubsub_zmq3", true, "enable PubSub service over ZeromMQ 3.x")
	enableZmq3Logging := flag.Bool("enable_logging_zmq3", true, "enable Logging service over ZeromMQ 3.x")
	enableWSRPC := flag.Bool("enable_rpc_ws", false, "enable RPC service over WebSocket")
	verbose := flag.Bool("v", false, "enable logging to stderr")
	help := flag.Bool("h", false, "print help and exit")

	flag.Parse()

	if *help {
		Usage()
	}

	if *verbose {
		ciderLog.UseLogger(seelog.Default)
		defer ciderLog.Flush()
	}

	// Instantiate Cider service exchanges.
	var (
		balancer = roundrobin.NewBalancer()
		eventBus = eventbus.New()
		logger   = publisher.New()
	)
	defer logger.Close()

	// Register a special inproc service endpoint.
	ciderTransport := rpc_inproc.NewTransport("Cider", balancer)
	ciderClient, err := rpc_client.NewService(func() (rpc_client.Transport, error) {
		return ciderTransport, nil
	})
	if err != nil {
		return err
	}

	// Register service endpoints with the broker.
	broker.RegisterEndpointFactory("cider_rpc_inproc", func() (broker.Endpoint, error) {
		log.Println("Configuring Cider management RPC inproc transport...")
		return ciderTransport.AsEndpoint(), nil
	})

	if *enableZmq3RPC {
		log.Println("Configuring ZeroMQ 3.x endpoint for RPC...")
		broker.RegisterEndpointFactory("zmq3_rpc", func() (broker.Endpoint, error) {
			config := zrpc.NewEndpointConfig()
			config.MustFeedFromEnv("CIDER_ZMQ3_RPC_").MustBeComplete()
			return zrpc.NewEndpoint(config, balancer)
		})
	}

	if *enableZmq3PubSub {
		log.Println("Configuring ZeroMQ 3.x endpoint for PubSub...")
		broker.RegisterEndpointFactory("zmq3_pubsub", func() (broker.Endpoint, error) {
			config := zpubsub.NewEndpointConfig()
			config.MustFeedFromEnv("CIDER_ZMQ3_PUBSUB_").MustBeComplete()
			return zpubsub.NewEndpoint(config, eventBus)
		})
	}

	if *enableZmq3Logging {
		log.Println("Configuring ZeroMQ 3.x endpoint for Logging...")
		broker.RegisterEndpointFactory("zmq3_logging", func() (broker.Endpoint, error) {
			config := zlogging.NewEndpointConfig()
			config.MustFeedFromEnv("CIDER_ZMQ3_LOGGING_").MustBeComplete()
			return zlogging.NewEndpoint(config, logger)
		})
	}

	if *enableWSRPC {
		log.Println("Configuring WebSocket endpoint for RPC...")
		broker.RegisterEndpointFactory("websocket_rpc", func() (broker.Endpoint, error) {
			config := wsrpc.NewEndpointConfig()
			config.MustFeedFromEnv("CIDER_WEBSOCKET_RPC_").MustBeComplete()
			if token := os.Getenv("CIDER_WEBSOCKET_RPC_TOKEN"); token != "" {
				config.WSHandshake = func(cfg *ws.Config, req *http.Request) error {
					if req.Header.Get("X-Cider-Token") != token {
						return errors.New("Invalid access token")
					}
					return nil
				}
			}
			return wsrpc.NewEndpoint(config, balancer)
		})
	}

	// Start catching signals.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Terminate ZeroMQ on exit.
	defer func() {
		log.Println("Waiting for ZeroMQ to terminate...")
		zmq.Term()
		log.Println("ZeroMQ terminated")
	}()

	// Register a monitoring channel.
	monitorCh := make(chan *broker.EndpointCrashReport)
	broker.Monitor(monitorCh)

	// Start all the registered service endpoints.
	log.Println("Broker configured, starting registered endpoints...")
	broker.ListenAndServe()
	defer broker.Terminate()

	// Start Cider apps.
	if *workspace == "" {
		*workspace = os.Getenv("CIDER_APPS_WORKSPACE")
	}

	supervisor, err := exec.NewSupervisor(*workspace)
	if err != nil {
		return err
	}
	supervisor.CloseAppStateChangeFeed()

	var factory apps.AppServiceFactory
	if err := factory.FeedConfigFromEnv("CIDER_APPS_"); err != nil {
		return err
	}
	if *workspace != "" {
		factory.Workspace = *workspace
	}
	if factory.DatabaseURL == "" || *dbURL != "localhost" {
		factory.DatabaseURL = *dbURL
	}

	appService, err := factory.NewAppService(supervisor, logger)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("Waiting for apps to terminate...")
		appService.TerminateWithin(AppsKillTimeout)
		log.Println("Apps terminated")
	}()

	// Export Cider management calls.
	if err := appService.ExportManagementMethods(ciderClient); err != nil {
		return err
	}

	// Loop until interrupted.
Loop:
	for {
		select {
		case err, ok := <-monitorCh:
			if !ok {
				break Loop
			}
			log.Printf("Endpoint %v crashed with error=%v\n", err.FactoryId, err.Error)
			if err.Dropped {
				log.Printf("Endpoint %v dropped", err.FactoryId)
			}
		case <-signalCh:
			log.Printf("Signal received, terminating...")
			break Loop
		}
	}

	return nil
}

func Usage() {
	fmt.Fprint(os.Stderr, `APPLICATION
  cider - A framework for connecting and managing distributed components

VERSION
`)
	fmt.Fprintf(os.Stderr, "  %s\n", broker.Version)
	fmt.Fprint(os.Stderr, `
USAGE
  cider [ OPTION ... ]

OPTIONS
`)
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, `
DESCRIPTION
  Cider incorporates two core components:
    1) an inter- and in-process messaging framework, and
    2) an application package manager and supervisor.

  The messaging framework can be used by Cider apps for talking to each other,
  but it is as well being used by the package manager to receive management
  requests. In this respect the package manager is a Cider application itself,
  although it is using a special inproc transport to connect to the messaging
  framework.

  The app package manager can be used for user-friendly application sources
  and processes management. Consult the ciderapp command line utility docs
  to see what operations are available.

`)
	os.Exit(2)
}
