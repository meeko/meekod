// Copyright (c) 2013 The cider AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package apps

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/cider/cider/apps/data"
	log "github.com/cider/cider/broker/exchanges/logging/publisher"
	"github.com/cider/go-cider/cider/services/rpc"
	"github.com/dmotylev/nutrition"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo"
	"os"
	"time"
)

type AppServiceFactory struct {
	Workspace   string
	DatabaseURL string
}

func (factory *AppServiceFactory) FeedConfigFromEnv(prefix string) error {
	return nutrition.Env(prefix).Feed(factory)
}

type AppService struct {
	apps          *mgo.Collection
	session       *mgo.Session
	token         []byte
	supervisor    Supervisor
	logs          *log.Publisher
	cmdChans      []chan rpc.RemoteRequest
	termCh        chan struct{}
	loopTermAckCh chan struct{}
	termAckCh     chan struct{}
}

const (
	AppStateStopped = "stopped"
	AppStateRunning = "running"
	AppStateCrashed = "crashed"
)

type AppStateChange struct {
	Alias     string
	FromState string
	ToState   string
}

type ActionContext interface {
	SignalProgress() error
	Stdout() io.Writer
	Stderr() io.Writer
	Interrupted() <-chan struct{}
}

type Supervisor interface {
	Install(alias string, repo string, ctx ActionContext) (*data.App, error)
	Upgrade(app *data.App, ctx ActionContext) error
	Remove(app *data.App, ctx ActionContext) error

	Start(app *data.App, ctx ActionContext) error
	Stop(alias string, ctx ActionContext) error
	StopWithTimeout(alias string, ctx ActionContext, timeout time.Duration) error
	Kill(alias string, ctx ActionContext) error
	Restart(app *data.App, ctx ActionContext) error
	Status(alias string, ctx ActionContext) (status string, err error)
	Statuses(ctx ActionContext) (statuses map[string]string, err error)

	AppStateChangeFeed() <-chan *AppStateChange
	CloseAppStateChangeFeed()

	Terminate(timeout time.Duration)
}

func (factory *AppServiceFactory) NewAppService(supervisor Supervisor, logs *log.Publisher) (*AppService, error) {
	// Make sure the required fields are set.
	if factory.Workspace == "" {
		return nil, &ErrNotDefined{"Cider workspace"}
	}
	if factory.DatabaseURL == "" {
		return nil, &ErrNotDefined{"Cider database URL"}
	}

	// Make sure the directory is there. Not sure it is writable for Cider,
	// that is not what MkdirAll checks.
	if err := os.MkdirAll(factory.Workspace, 0750); err != nil {
		return nil, err
	}

	// Read the management token from disk.
	token, err := ReadOrGenerateToken(factory.Workspace)
	if err != nil {
		return nil, err
	}

	// Connect to the database. No extended configuration supported for now.
	session, err := mgo.Dial(factory.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("MongoDB driver: %v", err)
	}

	session.SetMode(mgo.Strong, false)
	session.SetSafe(&mgo.Safe{})

	// Ensure database indexes.
	apps := session.DB("").C("apps")
	err = apps.EnsureIndex(mgo.Index{
		Key:    []string{"alias"},
		Unique: true,
	})
	if err != nil {
		session.Close()
		return nil, err
	}

	// Initialise a new AppService instance.
	srv := &AppService{
		apps:          apps,
		session:       session,
		token:         token,
		supervisor:    supervisor,
		logs:          logs,
		cmdChans:      make([]chan rpc.RemoteRequest, numCmds),
		termCh:        make(chan struct{}),
		loopTermAckCh: make(chan struct{}),
		termAckCh:     make(chan struct{}),
	}

	for i := range srv.cmdChans {
		srv.cmdChans[i] = make(chan rpc.RemoteRequest)
	}

	// Try to start apps that are supposed to be running.
	var app data.App
	ctx := newNilActionContext()
	iter := apps.Find(nil).Iter()
	for iter.Next(&app) {
		if app.Enabled {
			supervisor.Start(&app, ctx)
		}
	}
	if err := iter.Err(); err != nil {
		session.Close()
		return nil, err
	}

	// Return the newly created app service.
	go srv.loop()
	return srv, nil
}

func (srv *AppService) authenticate(token []byte) error {
	if !bytes.Equal(token, srv.token) {
		return errors.New("invalid token")
	}
	return nil
}

func (srv *AppService) TerminateWithin(timeout time.Duration) {
	select {
	case <-srv.termCh:
	default:
		close(srv.termCh)
		<-srv.loopTermAckCh
		srv.session.Close()
		srv.supervisor.Terminate(timeout)
		close(srv.termAckCh)
	}
}

func (srv *AppService) Terminated() <-chan struct{} {
	return srv.termAckCh
}

type nilActionContext struct{}

func newNilActionContext() ActionContext {
	return &nilActionContext{}
}

func (ctx *nilActionContext) SignalProgress() error {
	return nil
}

func (ctx *nilActionContext) Stdout() io.Writer {
	return ioutil.Discard
}

func (ctx *nilActionContext) Stderr() io.Writer {
	return ioutil.Discard
}

func (ctx *nilActionContext) Interrupted() <-chan struct{} {
	return nil
}

// Errors ----------------------------------------------------------------------

type ErrNotDefined struct {
	What string
}

func (err *ErrNotDefined) Error() string {
	return fmt.Sprintf("%s not defined", err.What)
}
