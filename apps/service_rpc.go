// Copyright (c) 2013 The cider AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package apps

import (
	"fmt"
	"github.com/cider/cider/apps/data"
	"github.com/cider/cider/broker/services/logging"
	"github.com/cider/go-cider/cider/services/rpc"
	"github.com/wsxiaoys/terminal/color"
	"io"
	"labix.org/v2/mgo/bson"
	"reflect"
)

const (
	cmdList = iota
	cmdInstall
	cmdUpgrade
	cmdRemove
	cmdInfo
	cmdEnv
	cmdSet
	cmdUnset
	cmdStart
	cmdStop
	cmdRestart
	cmdStatus
	cmdWatch
	numCmds
)

func (srv *AppService) ExportManagementMethods(client *rpc.Service) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	client.MustRegisterMethod("Cider.Apps.List", srv.handleList)
	client.MustRegisterMethod("Cider.Apps.Install", srv.handleInstall)
	client.MustRegisterMethod("Cider.Apps.Upgrade", srv.handleUpgrade)
	client.MustRegisterMethod("Cider.Apps.Remove", srv.handleRemove)

	client.MustRegisterMethod("Cider.Apps.Info", srv.handleInfo)
	client.MustRegisterMethod("Cider.Apps.Env", srv.handleEnv)
	client.MustRegisterMethod("Cider.Apps.Set", srv.handleSet)
	client.MustRegisterMethod("Cider.Apps.Unset", srv.handleUnset)

	client.MustRegisterMethod("Cider.Apps.Start", srv.handleStart)
	client.MustRegisterMethod("Cider.Apps.Stop", srv.handleStop)
	client.MustRegisterMethod("Cider.Apps.Restart", srv.handleRestart)
	client.MustRegisterMethod("Cider.Apps.Status", srv.handleStatus)
	client.MustRegisterMethod("Cider.Apps.Watch", srv.handleWatch)

	return
}

func (srv *AppService) enqueueCmdAndWait(cmd int, request rpc.RemoteRequest) {
	select {
	case srv.cmdChans[cmd] <- request:
	case <-request.Interrupted():
		request.Resolve(1, map[string]string{"error": "interrupted"})
	case <-srv.termCh:
		request.Resolve(2, map[string]string{"error": "terminated"})
	}

	<-request.Resolved()
}

func (srv *AppService) loop() {
	handlers := []rpc.RequestHandler{
		cmdList:    srv.safeHandleList,
		cmdInstall: srv.safeHandleInstall,
		cmdUpgrade: srv.safeHandleUpgrade,
		cmdRemove:  srv.safeHandleRemove,
		cmdInfo:    srv.safeHandleInfo,
		cmdEnv:     srv.safeHandleEnv,
		cmdSet:     srv.safeHandleSet,
		cmdUnset:   srv.safeHandleUnset,
		cmdStart:   srv.safeHandleStart,
		cmdStop:    srv.safeHandleStop,
		cmdRestart: srv.safeHandleRestart,
		cmdStatus:  srv.safeHandleStatus,
		cmdWatch:   srv.safeHandleWatch,
	}

	cases := make([]reflect.SelectCase, len(srv.cmdChans)+1)
	for i := range srv.cmdChans {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(srv.cmdChans[i]),
		}
	}
	termChIndex := len(cases) - 1
	cases[termChIndex] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(srv.termCh),
	}

	for {
		chosen, recv, _ := reflect.Select(cases)
		switch chosen {
		case termChIndex:
			close(srv.loopTermAckCh)
			return
		default:
			handlers[chosen](recv.Interface().(rpc.RemoteRequest))
		}
	}
}

// List ------------------------------------------------------------------------

func (srv *AppService) handleList(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdList, request)
}

func (srv *AppService) safeHandleList(request rpc.RemoteRequest) {
	var args data.ListArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.ListReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.ListReply{Error: err.Error()})
		return
	}

	var apps []data.App
	if err := srv.apps.Find(nil).All(&apps); err != nil {
		request.Resolve(5, data.ListReply{Error: err.Error()})
	}

	request.Resolve(0, data.ListReply{Apps: apps})
}

// Install ---------------------------------------------------------------------

func (srv *AppService) handleInstall(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdInstall, request)
}

func (srv *AppService) safeHandleInstall(request rpc.RemoteRequest) {
	var args data.InstallArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.InstallReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.InstallReply{Error: err.Error()})
		return
	}

	n, err := srv.apps.Find(bson.M{"alias": args.Alias}).Count()
	if err != nil {
		request.Resolve(5, data.InstallReply{Error: err.Error()})
		return
	}
	if n != 0 {
		request.Resolve(6, data.InstallReply{Error: "alias already taken"})
		return
	}

	app, err := srv.supervisor.Install(args.Alias, args.Repository, request)
	if err != nil {
		request.Resolve(7, data.InstallReply{Error: err.Error()})
		return
	}

	app.Id = bson.NewObjectId()

	color.Fprintf(request.Stdout(), "@{c}>>>@{|} Inserting the app database record ... ")
	if err := srv.apps.Insert(app); err != nil {
		fail(request.Stdout())
		srv.supervisor.Remove(app, request)
		request.Resolve(8, data.InstallReply{Error: err.Error()})
		return
	}
	ok(request.Stdout())

	request.Resolve(0, data.InstallReply{})
}

// Upgrade ---------------------------------------------------------------------

func (srv *AppService) handleUpgrade(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdUpgrade, request)
}

func (srv *AppService) safeHandleUpgrade(request rpc.RemoteRequest) {
	var args data.UpgradeArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.UpgradeReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.UpgradeReply{Error: err.Error()})
		return
	}

	var app data.App
	if err := srv.apps.Find(bson.M{"alias": args.Alias}).One(&app); err != nil {
		request.Resolve(5, data.UpgradeReply{Error: err.Error()})
		return
	}

	if err := srv.supervisor.Upgrade(&app, request); err != nil {
		request.Resolve(6, data.UpgradeReply{Error: err.Error()})
		return
	}

	request.Resolve(0, data.UpgradeReply{})
}

// Remove ----------------------------------------------------------------------

func (srv *AppService) handleRemove(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdRemove, request)
}

func (srv *AppService) safeHandleRemove(request rpc.RemoteRequest) {
	var args data.RemoveArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.RemoveReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.RemoveReply{Error: err.Error()})
		return
	}

	var app data.App
	if err := srv.apps.Find(bson.M{"alias": args.Alias}).One(&app); err != nil {
		request.Resolve(5, data.RemoveReply{Error: err.Error()})
		return
	}

	if err := srv.supervisor.Remove(&app, request); err != nil {
		request.Resolve(6, data.RemoveReply{Error: err.Error()})
		return
	}

	color.Fprint(request.Stdout(), "@{c}>>>@{|} Deleting the app database record ... ")
	if err := srv.apps.RemoveId(app.Id); err != nil {
		fail(request.Stdout())
		request.Resolve(7, data.RemoveReply{Error: err.Error()})
		return
	}
	ok(request.Stdout())

	request.Resolve(0, data.RemoveReply{})
}

// Info ------------------------------------------------------------------------

func (srv *AppService) handleInfo(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdInfo, request)
}

func (srv *AppService) safeHandleInfo(request rpc.RemoteRequest) {
	var args data.InfoArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.InfoReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.InfoReply{Error: err.Error()})
		return
	}

	var app data.App
	if err := srv.apps.Find(bson.M{"alias": args.Alias}).One(&app); err != nil {
		request.Resolve(5, data.InfoReply{Error: err.Error()})
		return
	}

	for _, v := range app.Vars {
		if v.Secret {
			v.Value = "<secret>"
		}
	}

	request.Resolve(0, data.InfoReply{App: app})
}

// Env -------------------------------------------------------------------------

func (srv *AppService) handleEnv(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdEnv, request)
}

func (srv *AppService) safeHandleEnv(request rpc.RemoteRequest) {
	var args data.EnvArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.EnvReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.EnvReply{Error: err.Error()})
		return
	}

	var app data.App
	err := srv.apps.Find(bson.M{"alias": args.Alias}).Select(bson.M{"vars": 1}).One(&app)
	if err != nil {
		request.Resolve(5, data.EnvReply{Error: err.Error()})
		return
	}

	for _, v := range app.Vars {
		if v.Secret {
			v.Value = "<secret>"
		}
	}

	request.Resolve(0, data.EnvReply{Vars: app.Vars})
}

// Set -------------------------------------------------------------------------

func (srv *AppService) handleSet(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdSet, request)
}

func (srv *AppService) safeHandleSet(request rpc.RemoteRequest) {
	var args data.SetArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.SetReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.SetReply{Error: err.Error()})
		return
	}

	var app data.App
	err := srv.apps.Find(bson.M{"alias": args.Alias}).Select(bson.M{"vars": 1}).One(&app)
	if err != nil {
		request.Resolve(5, data.SetReply{Error: err.Error()})
		return
	}

	vr, ok := app.Vars[args.Variable]
	if !ok {
		request.Resolve(6, data.SetReply{Error: "unknown variable"})
		return
	}

	if err := vr.Set(args.Value); err != nil {
		request.Resolve(7, data.SetReply{Error: err.Error()})
		return
	}

	err = srv.apps.UpdateId(app.Id, bson.M{"$set": bson.M{"vars." + args.Variable + ".value": vr.Value}})
	if err != nil {
		request.Resolve(8, data.SetReply{Error: err.Error()})
		return
	}

	request.Resolve(0, data.SetReply{})
}

// Unset -----------------------------------------------------------------------

func (srv *AppService) handleUnset(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdUnset, request)
}

func (srv *AppService) safeHandleUnset(request rpc.RemoteRequest) {
	var args data.UnsetArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.UnsetReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.UnsetReply{Error: err.Error()})
		return
	}

	var app data.App
	err := srv.apps.Find(bson.M{"alias": args.Alias}).Select(bson.M{"vars": 1}).One(&app)
	if err != nil {
		request.Resolve(5, data.UnsetReply{Error: err.Error()})
		return
	}

	vr, ok := app.Vars[args.Variable]
	if !ok {
		request.Resolve(6, data.UnsetReply{Error: "unknown variable"})
		return
	}

	if vr.Value != "" {
		err = srv.apps.UpdateId(app.Id, bson.M{"$set": bson.M{"vars." + args.Variable + ".value": ""}})
		if err != nil {
			request.Resolve(7, data.UnsetReply{Error: err.Error()})
			return
		}
	}

	request.Resolve(0, data.UnsetReply{})
}

// Start -----------------------------------------------------------------------

func (srv *AppService) handleStart(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdStart, request)
}

func (srv *AppService) safeHandleStart(request rpc.RemoteRequest) {
	var args data.StartArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.StartReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.StartReply{Error: err.Error()})
		return
	}

	var app data.App
	if err := srv.apps.Find(bson.M{"alias": args.Alias}).One(&app); err != nil {
		request.Resolve(5, data.StartReply{Error: err.Error()})
		return
	}

	if err := app.Vars.Filled(); err != nil {
		request.Resolve(6, data.StartReply{Error: err.Error()})
		return
	}

	err := srv.apps.UpdateId(app.Id, bson.M{"$set": bson.M{"enabled": true}})
	if err != nil {
		request.Resolve(7, data.StartReply{Error: err.Error()})
		return
	}

	if args.Watch {
		go srv.watchApp(args.Alias, logging.LevelUnset, request)
	}

	if err := srv.supervisor.Start(&app, request); err != nil {
		request.Resolve(8, data.StartReply{Error: err.Error()})
		return
	}

	if !args.Watch {
		request.Resolve(0, data.StartReply{})
	}
}

// Stop ------------------------------------------------------------------------

func (srv *AppService) handleStop(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdStop, request)
}

func (srv *AppService) safeHandleStop(request rpc.RemoteRequest) {
	var args data.StopArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.StopReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.StopReply{Error: err.Error()})
		return
	}

	if err := srv.supervisor.Stop(args.Alias, request); err != nil {
		request.Resolve(5, data.StopReply{Error: err.Error()})
		return
	}

	err := srv.apps.Update(bson.M{"alias": args.Alias}, bson.M{"$set": bson.M{"enabled": false}})
	if err != nil {
		request.Resolve(6, data.StopReply{Error: err.Error()})
		return
	}

	request.Resolve(0, data.StopReply{})
}

// Restart ---------------------------------------------------------------------

func (srv *AppService) handleRestart(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdRestart, request)
}

func (srv *AppService) safeHandleRestart(request rpc.RemoteRequest) {
	var args data.RestartArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.RestartReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.RestartReply{Error: err.Error()})
		return
	}

	var app data.App
	if err := srv.apps.Find(bson.M{"alias": args.Alias}).One(&app); err != nil {
		request.Resolve(5, data.RestartReply{Error: err.Error()})
		return
	}

	if err := srv.supervisor.Restart(&app, request); err != nil {
		request.Resolve(6, data.StartReply{Error: err.Error()})
		return
	}

	request.Resolve(0, data.RestartReply{})
}

// Status ----------------------------------------------------------------------

func (srv *AppService) handleStatus(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdStatus, request)
}

func (srv *AppService) safeHandleStatus(request rpc.RemoteRequest) {
	var args data.StatusArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.StatusReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.StatusReply{Error: err.Error()})
		return
	}

	var reply data.StatusReply
	if args.Alias != "" {
		status, err := srv.supervisor.Status(args.Alias, request)
		if err != nil {
			request.Resolve(5, data.StatusReply{Error: err.Error()})
			return
		}
		reply.Status = status
	} else {
		statuses, err := srv.supervisor.Statuses(request)
		if err != nil {
			request.Resolve(5, data.StatusReply{Error: err.Error()})
			return
		}
		reply.Statuses = statuses
	}

	request.Resolve(0, &reply)
}

// Watch -----------------------------------------------------------------------

func (srv *AppService) handleWatch(request rpc.RemoteRequest) {
	srv.enqueueCmdAndWait(cmdWatch, request)
}

func (srv *AppService) safeHandleWatch(request rpc.RemoteRequest) {
	if srv.logs == nil {
		request.Resolve(9, data.WatchReply{Error: "log watching is disabled"})
		return
	}

	var args data.WatchArgs
	if err := request.UnmarshalArgs(&args); err != nil {
		request.Resolve(3, data.WatchReply{Error: err.Error()})
		return
	}

	if err := srv.authenticate(args.Token); err != nil {
		request.Resolve(4, data.WatchReply{Error: err.Error()})
		return
	}

	status, err := srv.supervisor.Status(args.Alias, request)
	if err != nil {
		request.Resolve(5, data.WatchReply{Error: err.Error()})
		return
	}

	if status != AppStateRunning {
		request.Resolve(6, data.WatchReply{Error: ErrAppNotRunning.Error()})
		return
	}

	go srv.watchApp(args.Alias, logging.Level(args.Level), request)
}

// XXX: If someone stops the app while someone else is watching,
//      the logs just stop streaming. That could be improved.
func (srv *AppService) watchApp(alias string, level logging.Level, request rpc.RemoteRequest) {
	color.Fprintf(request.Stdout(), "@{c}>>>@{|} Streaming logs for application %s\n", alias)
	handle, err := srv.logs.Subscribe(alias, level,
		func(level logging.Level, record []byte) {
			fmt.Fprintf(request.Stdout(), "[%v] %s", level, string(record))
		})
	if err != nil {
		request.Resolve(9, data.WatchReply{Error: err.Error()})
		return
	}

	<-request.Interrupted()

	if err := srv.logs.Unsubscribe(handle); err != nil {
		request.Resolve(10, data.WatchReply{Error: err.Error()})
		return
	}

	request.Resolve(0, data.WatchReply{})
}

// Helpers

func ok(w io.Writer) {
	color.Fprintln(w, "@{g}OK@{|}")
}

func fail(w io.Writer) {
	color.Fprintln(w, "@{r}FAIL@{|}")
}

func newlineOk(w io.Writer) {
	color.Fprintln(w, "@{c}<<<@{|} @{g}OK@{|}")
}

func newlineFail(w io.Writer) {
	color.Fprintln(w, "@{c}<<<@{|} @{r}FAIL@{|}")
}
