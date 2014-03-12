// Copyright (c) 2013 The cider AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package exec

import (
	"encoding/json"
	"fmt"
	"github.com/cider/cider/apps"
	"github.com/cider/cider/apps/data"
	"github.com/cider/cider/apps/utils/executil"
	"github.com/cider/cider/apps/utils/vcsutil"
	"github.com/wsxiaoys/terminal/color"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Supervisor struct {
	ws string

	records map[string]*appRecord
	mu      *sync.Mutex

	feedCh       chan *apps.AppStateChange
	feedClosedCh chan struct{}
}

type appRecord struct {
	state   string
	process *os.Process
	termCh  chan struct{}
}

func NewSupervisor(workspace string) (*Supervisor, error) {
	if workspace == "" {
		return nil, &apps.ErrNotDefined{"Cider workspace"}
	}

	if err := os.MkdirAll(workspace, 0750); err != nil {
		return nil, err
	}

	return &Supervisor{
		ws:           workspace,
		records:      make(map[string]*appRecord),
		mu:           new(sync.Mutex),
		feedCh:       make(chan *apps.AppStateChange),
		feedClosedCh: make(chan struct{}),
	}, nil
}

func (supervisor *Supervisor) getOrCreateRecord(alias string) *appRecord {
	record, ok := supervisor.records[alias]
	if ok {
		return record
	}

	record = &appRecord{
		state: apps.AppStateStopped,
	}

	supervisor.records[alias] = record
	return record
}

// apps.Supervisor interface ---------------------------------------------------

func (supervisor *Supervisor) Install(alias string, repo string, ctx apps.ActionContext) (*data.App, error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	repoURL, err := url.Parse(repo)
	if err != nil {
		return nil, err
	}

	vcs, err := vcsutil.GetVCS(repoURL.Scheme)
	if err != nil {
		return nil, err
	}

	var (
		appDir     = supervisor.appDir(alias)
		stagingDir = supervisor.appStagingDir(alias)
	)

	color.Fprint(ctx.Stdout(), "@{c}>>>@{|} Creating the app workspace ... ")
	if err := os.Mkdir(appDir, 0750); err != nil {
		printFAIL(ctx.Stdout())
		return nil, err
	}
	printOK(ctx.Stdout())

	color.Fprintln(ctx.Stdout(), "@{c}>>>@{|} Cloning the app repository ... ")
	if err := vcs.Clone(repoURL, stagingDir, ctx); err != nil {
		newlineFAIL(ctx.Stdout())
		os.Remove(appDir)
		return nil, err
	}
	newlineOK(ctx.Stdout())

	color.Fprint(ctx.Stdout(), "@{c}>>>@{|} Reading app.json ... ")
	content, err := ioutil.ReadFile(filepath.Join(stagingDir, ".cider", "app.json"))
	if err != nil {
		printFAIL(ctx.Stdout())
		os.RemoveAll(appDir)
		return nil, err
	}

	var app data.App
	if err := json.Unmarshal(content, &app); err != nil {
		printFAIL(ctx.Stdout())
		os.RemoveAll(appDir)
		return nil, fmt.Errorf("Failed to parse app.json: %v", err)
	}
	printOK(ctx.Stdout())

	app.Alias = alias
	app.Repository = repo

	color.Fprint(ctx.Stdout(), "@{c}>>>@{|} Validating app.json ... ")
	if err := app.FillAndValidate(); err != nil {
		printFAIL(ctx.Stdout())
		os.RemoveAll(appDir)
		return nil, err
	}
	printOK(ctx.Stdout())

	srcDir := supervisor.appSrcDir(&app)
	color.Fprint(ctx.Stdout(), "@{c}>>>@{|} Moving files into place ... ")
	if err := os.MkdirAll(filepath.Dir(srcDir), 0750); err != nil {
		printFAIL(ctx.Stdout())
		os.RemoveAll(appDir)
		return nil, err
	}

	if err := os.Rename(stagingDir, srcDir); err != nil {
		printFAIL(ctx.Stdout())
		os.RemoveAll(appDir)
		return nil, err
	}
	printOK(ctx.Stdout())

	color.Fprintln(ctx.Stdout(), "@{c}>>>@{|} Running the install hook ... ")
	app.Alias = alias
	if err := supervisor.runHook(&app, "install", ctx); err != nil {
		newlineFAIL(ctx.Stdout())
		os.RemoveAll(appDir)
		return nil, err
	}
	newlineOK(ctx.Stdout())

	return &app, nil
}

func (supervisor *Supervisor) Upgrade(app *data.App, ctx apps.ActionContext) error {
	mustHaveAlias(app)
	mustHaveRepository(app)

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	repoURL, err := url.Parse(app.Repository)
	if err != nil {
		return err
	}

	vcs, err := vcsutil.GetVCS(repoURL.Scheme)
	if err != nil {
		return err
	}

	color.Fprintln(ctx.Stdout(), "@{c}>>>@{|} Pulling the app repository ... ")
	if err := vcs.Pull(repoURL, supervisor.appSrcDir(app), ctx); err != nil {
		newlineFAIL(ctx.Stdout())
		return err
	}
	newlineOK(ctx.Stdout())

	color.Fprintln(ctx.Stdout(), "@{c}>>>@{|} Running the upgrade hook ... ")
	if err := supervisor.runHook(app, "upgrade", ctx); err != nil {
		newlineFAIL(ctx.Stdout())
		return err
	}
	newlineOK(ctx.Stdout())

	color.Fprint(ctx.Stdout(), "@{c}>>>@{|} Restarting the app if running ... ")
	if err := supervisor.unsafeStop(app.Alias, ctx); err != nil {
		if err == apps.ErrUnknownAlias || err == apps.ErrAppNotRunning {
			printOK(ctx.Stdout())
			return nil
		}
		printFAIL(ctx.Stdout())
		return err
	}

	if err := supervisor.unsafeStart(app, ctx); err != nil {
		printFAIL(ctx.Stdout())
		return err
	}

	printOK(ctx.Stdout())
	return nil
}

func (supervisor *Supervisor) Remove(app *data.App, ctx apps.ActionContext) error {
	mustHaveAlias(app)

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	// The app must not be running.
	record, ok := supervisor.records[app.Alias]
	if ok && record.state == apps.AppStateRunning {
		return apps.ErrAppRunning
	}

	// Remove the app directory.
	color.Fprintf(ctx.Stdout(), "@{c}>>>@{|} Removing the app workspace ... ")
	if err := os.RemoveAll(supervisor.appDir(app.Alias)); err != nil {
		printFAIL(ctx.Stdout())
		return err
	}
	printOK(ctx.Stdout())

	// Delete the app record.
	delete(supervisor.records, app.Alias)
	return nil
}

func (supervisor *Supervisor) Start(app *data.App, ctx apps.ActionContext) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.unsafeStart(app, ctx)
}

func (supervisor *Supervisor) unsafeStart(app *data.App, ctx apps.ActionContext) error {
	mustHaveAlias(app)
	alias := app.Alias

	// Make sure the app record exists.
	record := supervisor.getOrCreateRecord(alias)

	// The app is not supposed to be running.
	if record.state == apps.AppStateRunning {
		return apps.ErrAppRunning
	}

	// Start the app.
	var (
		bin = supervisor.appBinDir(app)
		exe = supervisor.appExecutable(app)
		run = supervisor.appRunDir(app)
	)
	if err := os.MkdirAll(run, 0750); err != nil {
		return err
	}

	sep := make([]byte, utf8.RuneLen(os.PathListSeparator))
	utf8.EncodeRune(sep, os.PathListSeparator)
	path := strings.Join([]string{os.Getenv("PATH"), bin}, string(sep))

	cmd := exec.Command(exe)
	cmd.Env = []string{
		"PATH=" + path,
		"GOPATH=" + supervisor.appDir(app.Alias),
		"CIDER_ALIAS=" + alias,
	}
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "CIDER_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if app.Vars != nil {
		for k, v := range app.Vars {
			if v.Value != "" {
				cmd.Env = append(cmd.Env, k+"="+v.Value)
			}
		}
	}
	cmd.Dir = supervisor.appRunDir(app)

	if err := cmd.Start(); err != nil {
		return err
	}
	record.state = apps.AppStateRunning
	record.process = cmd.Process
	termCh := make(chan struct{})
	record.termCh = termCh
	supervisor.emitStateChange(alias, apps.AppStateStopped, apps.AppStateRunning)

	// Start a monitoring goroutine that waits for the process to exit.
	go func() {
		err := cmd.Wait()
		supervisor.mu.Lock()
		if err == nil {
			record.state = apps.AppStateStopped
			supervisor.emitStateChange(alias, apps.AppStateRunning, apps.AppStateStopped)
		} else {
			record.state = apps.AppStateCrashed
			supervisor.emitStateChange(alias, apps.AppStateRunning, apps.AppStateCrashed)
		}
		record.process = nil
		close(termCh)
		supervisor.mu.Unlock()
	}()

	return nil
}

func (supervisor *Supervisor) Stop(alias string, ctx apps.ActionContext) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.unsafeStop(alias, ctx)
}

func (supervisor *Supervisor) StopWithin(alias string, ctx apps.ActionContext, timeout time.Duration) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.unsafeStop(alias, ctx)
}

func (supervisor *Supervisor) unsafeStop(alias string, ctx apps.ActionContext) error {
	return supervisor.stopAppWithin(-1, alias, ctx)
}

func (supervisor *Supervisor) StopWithTimeout(alias string, ctx apps.ActionContext, timeout time.Duration) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.stopAppWithin(timeout, alias, ctx)
}

func (supervisor *Supervisor) Kill(alias string, ctx apps.ActionContext) error {
	return supervisor.StopWithTimeout(alias, ctx, 0)
}

func (supervisor *Supervisor) Restart(app *data.App, ctx apps.ActionContext) error {
	mustHaveAlias(app)

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	if err := supervisor.unsafeStop(app.Alias, ctx); err != nil {
		return err
	}

	return supervisor.unsafeStart(app, ctx)
}

func (supervisor *Supervisor) Status(alias string, ctx apps.ActionContext) (status string, err error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	record, ok := supervisor.records[alias]
	if !ok {
		err = apps.ErrUnknownAlias
		return
	}

	status = record.state
	return
}

func (supervisor *Supervisor) Statuses(ctx apps.ActionContext) (statuses map[string]string, err error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	statuses = make(map[string]string, len(supervisor.records))
	for k, v := range supervisor.records {
		statuses[k] = v.state
	}

	return
}

func (supervisor *Supervisor) AppStateChangeFeed() <-chan *apps.AppStateChange {
	return supervisor.feedCh
}

func (supervisor *Supervisor) CloseAppStateChangeFeed() {
	select {
	case <-supervisor.feedClosedCh:
	default:
		close(supervisor.feedClosedCh)
	}
}

func (supervisor *Supervisor) Terminate(timeout time.Duration) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	var wg sync.WaitGroup
	for alias, app := range supervisor.records {
		if app.state == apps.AppStateRunning {
			wg.Add(1)
			go func(appAlias string) {
				defer wg.Done()
				supervisor.stopAppWithin(timeout, appAlias, nil)
			}(alias)
		}
	}

	wg.Wait()
	return
}

// Private methods -------------------------------------------------------------

func (supervisor *Supervisor) runHook(app *data.App, hook string, ctx apps.ActionContext) error {
	var (
		appDir    = supervisor.appDir(app.Alias)
		appSrcDir = supervisor.appSrcDir(app)
		appBinDir = supervisor.appBinDir(app)
		appRunDir = supervisor.appRunDir(app)
	)

	exe := filepath.Join(appSrcDir, ".cider", "hooks", hook)
	cmd := exec.Command(exe)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"GOPATH=" + appDir,
		"CIDER_HOMEDIR=" + appDir,
		"CIDER_SRCDIR=" + filepath.Dir(appSrcDir),
		"CIDER_APP_SRCDIR=" + appSrcDir,
		"CIDER_BINDIR=" + appBinDir,
		"CIDER_RUNDIR=" + appRunDir,
	}
	cmd.Dir = appSrcDir
	cmd.Stdout = ctx.Stdout()
	cmd.Stderr = ctx.Stderr()

	return executil.Run(cmd, ctx.Interrupted())
}

func (supervisor *Supervisor) stopAppWithin(timeout time.Duration, alias string, ctx apps.ActionContext) error {
	record, ok := supervisor.records[alias]
	if !ok {
		return apps.ErrUnknownAlias
	}
	if record.state != apps.AppStateRunning {
		return apps.ErrAppNotRunning
	}

	// Timeout == 0 --> SIGKILL
	if timeout == 0 {
		fmt.Fprintf(ctx.Stdout(), "Killing application %s...\n", alias)
		if err := record.process.Signal(os.Kill); err != nil {
			return err
		}
		supervisor.mu.Unlock()
		<-record.termCh
		supervisor.mu.Lock()
		fmt.Fprintf(ctx.Stdout(), "Application %s killed\n", alias)
		return nil
	}

	// Timeout != 0 --> SIGINT and wait for timeout
	fmt.Fprintf(ctx.Stdout(), "Stopping application %s...\n", alias)
	if err := record.process.Signal(os.Interrupt); err != nil {
		return err
	}

	var timeoutCh <-chan time.Time
	if timeout != -1 {
		timeoutCh = time.After(timeout)
	}

	supervisor.mu.Unlock()
	defer supervisor.mu.Lock()

	select {
	case <-record.termCh:
		fmt.Fprintf(ctx.Stdout(), "Application %s stopped\n", alias)
	case <-ctx.Interrupted():
		return apps.ErrInterrupted
	case <-timeoutCh:
		supervisor.mu.Lock()
		fmt.Fprintf(ctx.Stdout(), "Killing application %s...\n", alias)
		if err := record.process.Signal(os.Kill); err != nil {
			supervisor.mu.Unlock()
			return err
		}
		supervisor.mu.Unlock()

		select {
		case <-record.termCh:
			fmt.Fprintf(ctx.Stdout(), "Application %s killed\n", alias)
		case <-ctx.Interrupted():
			return apps.ErrInterrupted
		}
	}

	return nil
}

func (supervisor *Supervisor) emitStateChange(alias, from, to string) {
	select {
	case supervisor.feedCh <- &apps.AppStateChange{alias, from, to}:
	case <-supervisor.feedClosedCh:
	}
}

func (supervisor *Supervisor) appDir(alias string) string {
	return filepath.Join(supervisor.ws, alias)
}

func (supervisor *Supervisor) appStagingDir(alias string) string {
	return filepath.Join(supervisor.ws, alias, "_stage")
}

func (supervisor *Supervisor) appSrcDir(app *data.App) string {
	mustHaveAlias(app)
	mustHaveName(app)
	return filepath.Join(supervisor.appDir(app.Alias), "src", app.Name)
}

func (supervisor *Supervisor) appBinDir(app *data.App) string {
	return filepath.Join(supervisor.appDir(app.Alias), "bin")
}

func (supervisor *Supervisor) appRunDir(app *data.App) string {
	mustHaveName(app)
	return filepath.Join(supervisor.appDir(app.Alias), "run")
}

func (supervisor *Supervisor) appExecutable(app *data.App) string {
	mustHaveName(app)
	return filepath.Join(supervisor.appBinDir(app), app.Name)
}

func mustHaveAlias(app *data.App) {
	if app.Alias == "" {
		panic("application alias not set")
	}
}

func mustHaveName(app *data.App) {
	if app.Name == "" {
		panic("application name not set")
	}
}

func mustHaveRepository(app *data.App) {
	if app.Repository == "" {
		panic("application repository not set")
	}
}

func printOK(w io.Writer) {
	color.Fprintln(w, "@{g}OK@{|}")
}

func printFAIL(w io.Writer) {
	color.Fprintln(w, "@{r}FAIL@{|}")
}

func newlineOK(w io.Writer) {
	color.Fprintln(w, "@{c}<<<@{|} @{g}OK@{|}")
}

func newlineFAIL(w io.Writer) {
	color.Fprintln(w, "@{c}<<<@{|} @{r}FAIL@{|}")
}
