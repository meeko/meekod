// Copyright (c) 2013 The meeko AUTHORS
//
// Use of this source code is governed by the MIT license
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

	// Meeko broker
	"github.com/meeko/meekod/broker"
	"github.com/meeko/meekod/broker/exchanges/logging/publisher"
	"github.com/meeko/meekod/broker/exchanges/pubsub/eventbus"
	"github.com/meeko/meekod/broker/exchanges/rpc/roundrobin"
	meekoLog "github.com/meeko/meekod/broker/log"
	wsrpc "github.com/meeko/meekod/broker/transports/websocket/rpc"
	zlogging "github.com/meeko/meekod/broker/transports/zmq3/logging"
	zpubsub "github.com/meeko/meekod/broker/transports/zmq3/pubsub"
	zrpc "github.com/meeko/meekod/broker/transports/zmq3/rpc"

	// Meeko client
	rpc_client "github.com/meeko/go-meeko/meeko/services/rpc"
	rpc_inproc "github.com/meeko/go-meeko/meeko/transports/inproc/rpc"

	// Meeko apps
	"github.com/meeko/meekod/supervisor"
	"github.com/meeko/meekod/supervisor/implementations/exec"

	// Others
	ws "code.google.com/p/go.net/websocket"
	"github.com/cihub/seelog"
	_ "github.com/joho/godotenv/autoload" // Load .env on init.
	zmq "github.com/pebbe/zmq3"
)

func main() {
	if err := runMeeko(); err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func runMeeko() error {
	// Get rid of the date and time log prefix.
	log.SetFlags(0)

	// Parse configuration.
	var (
		configPath   string = os.Getenv("MEEKOD_CONFIG")
		noSupervisor bool   = os.Getenv("MEEKOD_DISABLE_SUPERVISOR") != ""
		noZmq        bool   = os.Getenv("MEEKOD_DISABLE_ZMQ") != ""
		verbose      bool   = os.Getenv("MEEKOD_VERBOSE") != ""
	)
	flag.StringVar(&configPath, "config", configPath, "configuration file path")
	flag.BoolVar(&noSupervisor, "disable_supervisor", noSupervisor, "disable the agent supervisor")
	flag.BoolVar(&noZmq, "disable_zmq", noZmq, "disable all ZeroMQ 3.x service endpoints")
	flag.BoolVar(&verbose, "verbose", verbose, "enable logging to stderr")
	help := flag.Bool("h", false, "print help and exit")
	flag.Parse()

	if *help {
		Usage()
	}

	if verbose {
		meekoLog.UseLogger(seelog.Default)
		defer meekoLog.Flush()
	}

	// Read the config file.
	if configPath == "" {
		return errors.New("configuration file path not set")
	}

	config, err := readConfig(configPath)
	if err != nil {
		return err
	}

	if !noSupervisor {
		if err := config.EnsureSupervisorConfig(); err != nil {
			return err
		}
	}
	if !noZmq {
		if err := config.EnsureZmqConfig(); err != nil {
			return err
		}
	}

	// Instantiate Meeko service exchanges.
	var (
		balancer = roundrobin.NewBalancer()
		eventBus = eventbus.New()
		logger   = publisher.New()
	)
	defer logger.Close()

	// Register a special inproc service endpoint.
	var inprocClient *rpc_client.Service
	if !noSupervisor {
		transport := rpc_inproc.NewTransport("Meeko", balancer)
		inprocClient, err = rpc_client.NewService(func() (rpc_client.Transport, error) {
			return transport, nil
		})
		if err != nil {
			return err
		}

		// Register service endpoints with the broker.
		broker.RegisterEndpointFactory("meeko_rpc_inproc", func() (broker.Endpoint, error) {
			log.Println("Configuring Meeko management RPC inproc transport...")
			return transport.AsEndpoint(), nil
		})
	}

	if !noZmq {
		// Register all the ZeroMQ service endpoints.
		log.Println("Configuring ZeroMQ 3.x endpoint for RPC...")
		broker.RegisterEndpointFactory("zmq3_rpc", func() (broker.Endpoint, error) {
			cfg := zrpc.NewEndpointConfig()
			cfg.Endpoint = config.Broker.Endpoints.RPC.ZeroMQ
			cfg.MustBeComplete()
			return zrpc.NewEndpoint(cfg, balancer)
		})

		log.Println("Configuring ZeroMQ 3.x endpoint for PubSub...")
		broker.RegisterEndpointFactory("zmq3_pubsub", func() (broker.Endpoint, error) {
			cfg := zpubsub.NewEndpointConfig()
			cfg.RouterEndpoint = config.Broker.Endpoints.PubSub.ZeroMQ.Router
			cfg.PubEndpoint = config.Broker.Endpoints.PubSub.ZeroMQ.Pub
			cfg.MustBeComplete()
			return zpubsub.NewEndpoint(cfg, eventBus)
		})

		log.Println("Configuring ZeroMQ 3.x endpoint for Logging...")
		broker.RegisterEndpointFactory("zmq3_logging", func() (broker.Endpoint, error) {
			cfg := zlogging.NewEndpointConfig()
			cfg.Endpoint = config.Broker.Endpoints.Logging.ZeroMQ
			cfg.MustBeComplete()
			return zlogging.NewEndpoint(cfg, logger)
		})

		// Terminate ZeroMQ on exit.
		defer func() {
			log.Println("Waiting for ZeroMQ to terminate...")
			zmq.Term()
			log.Println("ZeroMQ terminated")
		}()
	} else {
		// Terminate ZeroMQ now.
		zmq.Term()
	}

	// Register the WebSocket RPC endpoint. This one is always enabled
	// so that the management utility can connect to Meeko.
	log.Println("Configuring WebSocket endpoint for RPC...")
	broker.RegisterEndpointFactory("websocket_rpc", func() (broker.Endpoint, error) {
		cfg := wsrpc.NewEndpointConfig()
		cfg.Addr = config.Broker.Endpoints.RPC.WebSocket.Address
		token := config.Broker.Endpoints.RPC.WebSocket.Token
		cfg.WSHandshake = func(cfg *ws.Config, req *http.Request) error {
			if req.Header.Get("X-Meeko-Token") != token {
				return errors.New("Invalid access token")
			}
			return nil
		}
		return wsrpc.NewEndpoint(cfg, balancer)
	})

	// Start catching signals.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Register a monitoring channel.
	monitorCh := make(chan *broker.EndpointCrashReport)
	broker.Monitor(monitorCh)

	// Start all the registered service endpoints.
	log.Println("Broker configured, starting registered endpoints...")
	broker.ListenAndServe()
	defer broker.Terminate()

	if !noSupervisor {
		// Start the supervisor.
		supImpl, err := exec.NewSupervisor(config.Supervisor.Workspace)
		if err != nil {
			return err
		}
		// TODO: This is a weird method name and anyway, do we need it?
		//       There should be probably a method for enabling such a channel.
		supImpl.CloseAgentStateChangeFeed()

		sup, err := supervisor.New(
			supImpl,
			config.Supervisor.Workspace,
			config.Supervisor.MongoDbUrl,
			config.Supervisor.Token,
			logger)
		if err != nil {
			return err
		}
		defer func() {
			log.Println("Waiting for the agents to terminate...")
			sup.Terminate()
			log.Println("Agents terminated")
		}()

		// Export Meeko management calls.
		if err := sup.ExportManagementMethods(inprocClient); err != nil {
			return err
		}
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
		case sig := <-signalCh:
			log.Printf("Signal received (%v), terminating...", sig)
			break Loop
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
  MEEKOD_DISABLE_ZMQ
  MEEKOD_VERBOSE

  The meaning of these environment variables is the same as their flag
  counterparts. A variable is evaluated as true when it is set.

`)
	os.Exit(2)
}
