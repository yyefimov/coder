package autostart

import (
	"context"
	"fmt"
	"io"
	"time"

	"golang.org/x/xerrors"

	"cdr.dev/slog"
	"cdr.dev/slog/sloggers/sloghuman"
	"github.com/coder/coder/v2/coderd/tracing"
	"github.com/coder/coder/v2/codersdk"
	"github.com/coder/coder/v2/scaletest/createusers"
	"github.com/coder/coder/v2/scaletest/harness"
	"github.com/coder/coder/v2/scaletest/loadtestutil"
	"github.com/coder/coder/v2/scaletest/workspacebuild"
)

type Runner struct {
	client *codersdk.Client
	cfg    Config

	createUserRunner     *createusers.Runner
	workspacebuildRunner *workspacebuild.Runner

	autostartDuration time.Duration
	// Closed when the autostart schedule has been set.
	// Used in tests.
	autostartSet chan struct{}
}

func NewRunner(client *codersdk.Client, cfg Config) *Runner {
	return &Runner{
		client:       client,
		cfg:          cfg,
		autostartSet: make(chan struct{}),
	}
}

var (
	_ harness.Runnable    = &Runner{}
	_ harness.Cleanable   = &Runner{}
	_ harness.Collectable = &Runner{}
)

func (r *Runner) Run(ctx context.Context, id string, logs io.Writer) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	logs = loadtestutil.NewSyncWriter(logs)
	logger := slog.Make(sloghuman.Sink(logs)).Leveled(slog.LevelDebug)
	r.client.SetLogger(logger)
	r.client.SetLogBodies(true)

	r.createUserRunner = createusers.NewRunner(r.client, r.cfg.User)
	newUserAndToken, err := r.createUserRunner.RunReturningUser(ctx, id, logs)
	if err != nil {
		r.cfg.Metrics.AddError("", "create_user")
		return xerrors.Errorf("create user: %w", err)
	}
	newUser := newUserAndToken.User

	newUserClient := codersdk.New(r.client.URL,
		codersdk.WithSessionToken(newUserAndToken.SessionToken),
		codersdk.WithLogger(logger),
		codersdk.WithLogBodies())

	logger.Info(ctx, fmt.Sprintf("user %q created", newUser.Username), slog.F("id", newUser.ID.String()))

	workspaceBuildConfig := r.cfg.Workspace
	workspaceBuildConfig.OrganizationID = r.cfg.User.OrganizationID
	workspaceBuildConfig.UserID = newUser.ID.String()

	r.workspacebuildRunner = workspacebuild.NewRunner(newUserClient, workspaceBuildConfig)
	workspace, err := r.workspacebuildRunner.RunReturningWorkspace(ctx, id, logs)
	if err != nil {
		r.cfg.Metrics.AddError(newUser.Username, "create_workspace")
		return xerrors.Errorf("create workspace: %w", err)
	}

	logger.Info(ctx, fmt.Sprintf("workspace %q created", workspace.Name))

	logger.Info(ctx, fmt.Sprintf("stopping workspace %q", workspace.Name))

	stopBuild, err := newUserClient.CreateWorkspaceBuild(ctx, workspace.ID, codersdk.CreateWorkspaceBuildRequest{
		Transition: codersdk.WorkspaceTransitionStop,
	})
	if err != nil {
		r.cfg.Metrics.AddError(newUser.Username, "create_stop_build")
		return xerrors.Errorf("create stop build: %w", err)
	}

	stopBuildCtx, cancel2 := context.WithTimeout(ctx, r.cfg.WorkspaceJobTimeout)
	defer cancel2()

	err = workspacebuild.WaitForBuild(stopBuildCtx, logs, newUserClient, stopBuild.ID)
	if err != nil {
		r.cfg.Metrics.AddError(newUser.Username, "wait_for_stop_build")
		return xerrors.Errorf("wait for stop build to complete: %w", err)
	}

	logger.Info(ctx, fmt.Sprintf("workspace %q stopped successfully", workspace.Name))

	logger.Info(ctx, "waiting for all runners to reach barrier")
	r.cfg.SetupBarrier.Done()
	r.cfg.SetupBarrier.Wait()
	logger.Info(ctx, "all runners reached barrier, proceeding with autostart schedule")

	autoStartTime := r.cfg.Clock.Now().Add(r.cfg.AutostartDelay)
	schedule := fmt.Sprintf("CRON_TZ=UTC %d %d * * *", autoStartTime.Minute(), autoStartTime.Hour())

	logger.Info(ctx, fmt.Sprintf("setting autostart schedule for workspace %q: %s", workspace.Name, schedule))

	err = newUserClient.UpdateWorkspaceAutostart(ctx, workspace.ID, codersdk.UpdateWorkspaceAutostartRequest{
		Schedule: &schedule,
	})
	if err != nil {
		r.cfg.Metrics.AddError(newUser.Username, "update_workspace_autostart")
		return xerrors.Errorf("update workspace autostart: %w", err)
	}
	close(r.autostartSet)

	logger.Info(ctx, fmt.Sprintf("autostart schedule set for workspace %q", workspace.Name))

	logger.Info(ctx, fmt.Sprintf("waiting for workspace %q to autostart", workspace.Name))

	autostartInitiateCtx, cancel2 := context.WithTimeout(ctx, r.cfg.AutostartTimeout+r.cfg.AutostartDelay)
	defer cancel2()

	workspaceUpdates, err := newUserClient.WatchWorkspace(autostartInitiateCtx, workspace.ID)
	if err != nil {
		r.cfg.Metrics.AddError(newUser.Username, "watch_workspace")
		return xerrors.Errorf("watch workspace: %w", err)
	}

	var autoStartBuild codersdk.WorkspaceBuild

	logger.Info(ctx, "listening for workspace updates to detect autostart build")
waitNewBuildLoop:
	for {
		select {
		case <-autostartInitiateCtx.Done():
			return xerrors.Errorf("timeout waiting for autostart build to be created: %w", autostartInitiateCtx.Err())
		case updatedWorkspace, ok := <-workspaceUpdates:
			if !ok {
				r.cfg.Metrics.AddError(newUser.Username, "workspace_updates_channel_closed")
				return xerrors.Errorf("workspace updates channel closed")
			}

			if updatedWorkspace.LatestBuild.ID != stopBuild.ID &&
				updatedWorkspace.LatestBuild.Transition == codersdk.WorkspaceTransitionStart {
				autoStartBuild = updatedWorkspace.LatestBuild
				logger.Info(ctx, fmt.Sprintf("autostart build created with ID %s", autoStartBuild.ID))
				break waitNewBuildLoop
			}
		}
	}

	logger.Info(ctx, "waiting for autostart build to complete")
	buildCompleteCtx, cancel3 := context.WithTimeout(ctx, r.cfg.WorkspaceJobTimeout)
	defer cancel3()

	err = workspacebuild.WaitForBuild(buildCompleteCtx, logs, newUserClient, autoStartBuild.ID)
	if err != nil {
		r.cfg.Metrics.AddError(newUser.Username, "wait_for_autostart_build")
		return xerrors.Errorf("wait for autostart build to complete: %w", err)
	}

	r.autostartDuration = r.cfg.Clock.Since(autoStartTime)
	logger.Info(ctx, fmt.Sprintf("workspace %q autostarted successfully", workspace.Name))

	logger.Info(ctx, fmt.Sprintf("autostart completed in %v", r.autostartDuration))
	r.cfg.Metrics.RecordCompletion(r.autostartDuration, newUser.Username, workspace.Name)

	return nil
}

func (r *Runner) Cleanup(ctx context.Context, id string, logs io.Writer) error {
	if r.workspacebuildRunner != nil {
		_, _ = fmt.Fprintln(logs, "Cleaning up workspace...")
		if err := r.workspacebuildRunner.Cleanup(ctx, id, logs); err != nil {
			return xerrors.Errorf("cleanup workspace: %w", err)
		}
	}

	if r.createUserRunner != nil {
		_, _ = fmt.Fprintln(logs, "Cleaning up user...")
		if err := r.createUserRunner.Cleanup(ctx, id, logs); err != nil {
			return xerrors.Errorf("cleanup user: %w", err)
		}
	}

	return nil
}

const (
	AutostartLatencyMetric = "autostart_latency_seconds"
)

func (r *Runner) GetMetrics() map[string]any {
	return map[string]any{
		AutostartLatencyMetric: r.autostartDuration.Seconds(),
	}
}
