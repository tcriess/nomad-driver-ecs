package ecs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-driver-ecs/version"
	"github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
	"github.com/hashicorp/nomad/helper/pluginutils/hclutils"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
)

const (
	// pluginName is the name of the plugin.
	pluginName = "ecs"

	// fingerprintPeriod is the interval at which the driver will send
	// fingerprint responses.
	fingerprintPeriod = 30 * time.Second

	// taskHandleVersion is the version of task handle which this plugin sets
	// and understands how to decode. This is used to allow modification and
	// migration of the task schema used by the plugin.
	taskHandleVersion = 1
)

var (
	// pluginInfo is the response returned for the PluginInfo RPC.
	pluginInfo = &base.PluginInfoResponse{
		Type:              base.PluginTypeDriver,
		PluginApiVersions: []string{drivers.ApiVersion010},
		PluginVersion:     version.Version,
		Name:              pluginName,
	}

	// pluginConfigSpec is the hcl specification returned by the ConfigSchema RPC.
	pluginConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"enabled": hclspec.NewAttr("enabled", "bool", false),
		"cluster": hclspec.NewAttr("cluster", "string", false),
		"region":  hclspec.NewAttr("region", "string", false),
	})

	// taskConfigSpec represents an ECS task configuration object.
	// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/scheduling_tasks.html
	taskConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"task":                   hclspec.NewBlock("task", false, awsECSTaskConfigSpec),
		"advertise":              hclspec.NewAttr("advertise", "bool", false),
		"port_map":               hclspec.NewAttr("port_map", "list(map(number))", false),
		"start_timeout":          hclspec.NewAttr("start_timeout", "string", false),
		"inline_task_definition": hclspec.NewAttr("inline_task_definition", "bool", false),
		"task_definition":        hclspec.NewBlock("task_definition", false, awsECSTaskDefinitionSpec),
	})

	// awsECSTaskConfigSpec are the high level configuration options for
	// configuring and ECS task.
	awsECSTaskConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"launch_type":           hclspec.NewAttr("launch_type", "string", false),
		"task_definition":       hclspec.NewAttr("task_definition", "string", false),
		"network_configuration": hclspec.NewBlock("network_configuration", false, awsECSNetworkConfigSpec),
		"tags":                  hclspec.NewAttr("tags", "list(map(string))", false),
	})

	// awsECSNetworkConfigSpec is the network configuration for the task.
	awsECSNetworkConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"aws_vpc_configuration": hclspec.NewBlock("aws_vpc_configuration", false, awsECSVPCConfigSpec),
	})

	// awsECSVPCConfigSpec is the object representing the networking details
	// for an ECS task or service.
	awsECSVPCConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"assign_public_ip": hclspec.NewAttr("assign_public_ip", "string", false),
		"security_groups":  hclspec.NewAttr("security_groups", "list(string)", false),
		"subnets":          hclspec.NewAttr("subnets", "list(string)", false),
	})

	awsECSContainerDefinitionSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"cpu":           hclspec.NewAttr("cpu", "number", true),
		"memory":        hclspec.NewAttr("memory", "number", true),
		"command":       hclspec.NewAttr("command", "list(string)", false),
		"entry_point":   hclspec.NewAttr("entry_point", "list(string)", false),
		"environment":   hclspec.NewAttr("environment", "list(map(string))", false),
		"image":         hclspec.NewAttr("image", "string", true),
		"port_mappings": hclspec.NewAttr("port_mappings", "list(map(number))", false),
	})

	awsECSTaskDefinitionSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"family":                hclspec.NewAttr("family", "string", true),
		"cpu":                   hclspec.NewAttr("cpu", "string", true),
		"memory":                hclspec.NewAttr("memory", "string", true),
		"execution_role_arn":    hclspec.NewAttr("execution_role_arn", "string", true),
		"task_role_arn":         hclspec.NewAttr("task_role_arn", "string", true),
		"container_definitions": hclspec.NewBlockList("container_definitions", awsECSContainerDefinitionSpec),
	})

	// capabilities is returned by the Capabilities RPC and indicates what
	// optional features this driver supports
	capabilities = &drivers.Capabilities{
		SendSignals: false,
		Exec:        false,
		FSIsolation: drivers.FSIsolationImage,
		RemoteTasks: true,
	}
)

// Driver is a driver for running ECS containers
type Driver struct {
	// eventer is used to handle multiplexing of TaskEvents calls such that an
	// event can be broadcast to all callers
	eventer *eventer.Eventer

	// config is the driver configuration set by the SetConfig RPC
	config *DriverConfig

	// nomadConfig is the client config from nomad
	nomadConfig *base.ClientDriverConfig

	// tasks is the in memory datastore mapping taskIDs to rawExecDriverHandles
	tasks *taskStore

	// ctx is the context for the driver. It is passed to other subsystems to
	// coordinate shutdown
	ctx context.Context

	// signalShutdown is called when the driver is shutting down and cancels the
	// ctx passed to any subsystems
	signalShutdown context.CancelFunc

	// logger will log to the Nomad agent
	logger hclog.Logger

	// ecsClientInterface is the interface used for communicating with AWS ECS
	client ecsClientInterface
}

// DriverConfig is the driver configuration set by the SetConfig RPC call
type DriverConfig struct {
	Enabled bool   `codec:"enabled"`
	Cluster string `codec:"cluster"`
	Region  string `codec:"region"`
}

// TaskConfig is the driver configuration of a task within a job
type TaskConfig struct {
	Task                 ECSTaskConfig      `codec:"task"`
	PortMap              hclutils.MapStrInt `codec:"port_map"`
	Advertise            bool               `codec:"advertise"`
	StartTimeout         string             `codec:"start_timeout"`
	InlineTaskDefinition bool               `codec:"inline_task_definition"`
	TaskDefinition       ECSTaskDefinition  `codec:"task_definition"`
}

const (
	defaultStartTimeout = 5 * time.Minute
)

type ECSTaskConfig struct {
	LaunchType           string                   `codec:"launch_type"`
	TaskDefinition       string                   `codec:"task_definition"`
	NetworkConfiguration TaskNetworkConfiguration `codec:"network_configuration"`
	Tags                 []Tag                    `codec:"tags"`
}

type ECSTaskDefinition struct {
	Family               string                   `codec:"family"`
	Cpu                  string                   `codec:"cpu"`
	Memory               string                   `codec:"memory"`
	ExecutionRoleArn     string                   `codec:"execution_role_arn"`
	TaskRoleArn          string                   `codec:"task_role_arn"`
	ContainerDefinitions []ECSContainerDefinition `codec:"container_definitions"`
}

type ECSContainerDefinition struct {
	Cpu          int64              `codec:"cpu"`
	Memory       int64              `codec:"memory"`
	Command      []string           `codec:"command"`
	EntryPoint   []string           `codec:"entry_point"`
	Environment  []Tag              `codec:"environment"`
	Image        string             `codec:"image"`
	PortMappings hclutils.MapStrInt `codec:"port_mappings"`
}

type Tag struct {
	Key   string `codec:"key"`
	Value string `codec:"value"`
}

type TaskNetworkConfiguration struct {
	TaskAWSVPCConfiguration TaskAWSVPCConfiguration `codec:"aws_vpc_configuration"`
}

type TaskAWSVPCConfiguration struct {
	AssignPublicIP string   `codec:"assign_public_ip"`
	SecurityGroups []string `codec:"security_groups"`
	Subnets        []string `codec:"subnets"`
}

// TaskState is the state which is encoded in the handle returned in
// StartTask. This information is needed to rebuild the task state and handler
// during recovery.
type TaskState struct {
	TaskConfig    *drivers.TaskConfig
	ContainerName string
	ARN           string
	StartedAt     time.Time
	DriverNetwork *drivers.DriverNetwork
}

// NewECSDriver returns a new DriverPlugin implementation
func NewPlugin(logger hclog.Logger) drivers.DriverPlugin {
	ctx, cancel := context.WithCancel(context.Background())
	logger = logger.Named(pluginName)
	return &Driver{
		eventer:        eventer.NewEventer(ctx, logger),
		config:         &DriverConfig{},
		tasks:          newTaskStore(),
		ctx:            ctx,
		signalShutdown: cancel,
		logger:         logger,
	}
}

func (d *Driver) PluginInfo() (*base.PluginInfoResponse, error) {
	return pluginInfo, nil
}

func (d *Driver) ConfigSchema() (*hclspec.Spec, error) {
	return pluginConfigSpec, nil
}

func (d *Driver) SetConfig(cfg *base.Config) error {
	var config DriverConfig
	if len(cfg.PluginConfig) != 0 {
		if err := base.MsgPackDecode(cfg.PluginConfig, &config); err != nil {
			return err
		}
	}

	d.config = &config
	if cfg.AgentConfig != nil {
		d.nomadConfig = cfg.AgentConfig.Driver
	}

	client, err := d.getAwsSdk(config.Cluster)
	if err != nil {
		return fmt.Errorf("failed to get AWS SDK client: %v", err)
	}
	d.client = client

	return nil
}

func (d *Driver) getAwsSdk(cluster string) (ecsClientInterface, error) {
	awsCfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %v", err)
	}

	if d.config.Region != "" {
		awsCfg.Region = d.config.Region
	}

	return awsEcsClient{
		cluster:   cluster,
		ecsClient: ecs.New(awsCfg),
	}, nil
}

func (d *Driver) Shutdown(ctx context.Context) error {
	d.signalShutdown()
	return nil
}

func (d *Driver) TaskConfigSchema() (*hclspec.Spec, error) {
	return taskConfigSpec, nil
}

func (d *Driver) Capabilities() (*drivers.Capabilities, error) {
	return capabilities, nil
}

func (d *Driver) Fingerprint(ctx context.Context) (<-chan *drivers.Fingerprint, error) {
	ch := make(chan *drivers.Fingerprint)
	go d.handleFingerprint(ctx, ch)
	return ch, nil
}

func (d *Driver) handleFingerprint(ctx context.Context, ch chan<- *drivers.Fingerprint) {
	defer close(ch)
	ticker := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			ticker.Reset(fingerprintPeriod)
			ch <- d.buildFingerprint(ctx)
		}
	}
}

func (d *Driver) buildFingerprint(ctx context.Context) *drivers.Fingerprint {
	var health drivers.HealthState
	var desc string
	attrs := map[string]*pstructs.Attribute{}

	if d.config.Enabled {
		if err := d.client.DescribeCluster(ctx); err != nil {
			health = drivers.HealthStateUnhealthy
			desc = err.Error()
			attrs["driver.ecs"] = pstructs.NewBoolAttribute(false)
		} else {
			health = drivers.HealthStateHealthy
			desc = "Healthy"
			attrs["driver.ecs"] = pstructs.NewBoolAttribute(true)
		}
	} else {
		health = drivers.HealthStateUndetected
		desc = "disabled"
	}

	return &drivers.Fingerprint{
		Attributes:        attrs,
		Health:            health,
		HealthDescription: desc,
	}
}

func (d *Driver) RecoverTask(handle *drivers.TaskHandle) error {
	d.logger.Info("recovering ecs task", "version", handle.Version,
		"task_config.id", handle.Config.ID, "task_state", handle.State,
		"driver_state_bytes", len(handle.DriverState))
	if handle == nil {
		return fmt.Errorf("handle cannot be nil")
	}

	// If already attached to handle there's nothing to recover.
	if _, ok := d.tasks.Get(handle.Config.ID); ok {
		d.logger.Info("no ecs task to recover; task already exists",
			"task_id", handle.Config.ID,
			"task_name", handle.Config.Name,
		)
		return nil
	}

	// Handle doesn't already exist, try to reattach
	var taskState TaskState
	if err := handle.GetDriverState(&taskState); err != nil {
		d.logger.Error("failed to decode task state from handle", "error", err, "task_id", handle.Config.ID)
		return fmt.Errorf("failed to decode task state from handle: %v", err)
	}

	d.logger.Info("ecs task recovered", "arn", taskState.ARN,
		"started_at", taskState.StartedAt)

	h := newTaskHandle(d.logger, taskState, handle.Config, d.client, taskState.DriverNetwork)

	d.tasks.Set(handle.Config.ID, h)

	go h.run()
	return nil
}

type startResult struct {
	ip     string
	status string
	err    error
}

func (d *Driver) StartTask(cfg *drivers.TaskConfig) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	if !d.config.Enabled {
		return nil, nil, fmt.Errorf("disabled")
	}

	if _, ok := d.tasks.Get(cfg.ID); ok {
		return nil, nil, fmt.Errorf("task with ID %q already started", cfg.ID)
	}

	var driverConfig TaskConfig
	if err := cfg.DecodeDriverConfig(&driverConfig); err != nil {
		return nil, nil, fmt.Errorf("failed to decode driver config: %v", err)
	}

	d.logger.Info("starting ecs task", "driver_cfg", hclog.Fmt("%+v", driverConfig))
	handle := drivers.NewTaskHandle(taskHandleVersion)
	handle.Config = cfg

	startTimeout, err := time.ParseDuration(driverConfig.StartTimeout)
	if err != nil || startTimeout <= 0 {
		d.logger.Info("invalid start timeout, setting default", "err", err.Error(), "default", defaultStartTimeout)
		startTimeout = defaultStartTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	if driverConfig.InlineTaskDefinition {
		containerDefinitions := make([]ecs.ContainerDefinition, len(driverConfig.TaskDefinition.ContainerDefinitions))
		for i, cd := range driverConfig.TaskDefinition.ContainerDefinitions {
			env := make([]ecs.KeyValuePair, len(cd.Environment))
			for j, e := range cd.Environment {
				env[j] = ecs.KeyValuePair{
					Name:  aws.String(e.Key),
					Value: aws.String(e.Value),
				}
			}
			pm := make([]ecs.PortMapping, len(cd.PortMappings))
			for jStr, p := range cd.PortMappings {
				j, err := strconv.ParseUint(jStr, 10, 64)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to convert port mapping index to int: %v", err)
				}
				pm[j] = ecs.PortMapping{
					ContainerPort: aws.Int64(int64(p)),
				}
			}
			containerDefinitions[i] = ecs.ContainerDefinition{
				Cpu:          aws.Int64(cd.Cpu),
				Memory:       aws.Int64(cd.Memory),
				Command:      cd.Command,
				EntryPoint:   cd.EntryPoint,
				Environment:  env,
				Image:        aws.String(cd.Image),
				PortMappings: pm,
			}
		}
		same, err := d.client.CheckTaskDefinition(ctx, driverConfig.TaskDefinition.Family, containerDefinitions, driverConfig.TaskDefinition.Cpu, driverConfig.TaskDefinition.Memory, driverConfig.TaskDefinition.ExecutionRoleArn, driverConfig.TaskDefinition.TaskRoleArn)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check ECS task definition: %v", err)
		}
		if !same {
			// the task definition is new, so we create it here
			familyVersion, err := d.client.RegisterTaskDefinition(ctx, driverConfig.TaskDefinition.Family, containerDefinitions, driverConfig.TaskDefinition.Cpu, driverConfig.TaskDefinition.Memory, driverConfig.TaskDefinition.ExecutionRoleArn, driverConfig.TaskDefinition.TaskRoleArn)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to register ECS task definition: %v", err)
			}
			d.logger.Info("new task definition version", "family", driverConfig.TaskDefinition.Family, "version", familyVersion)
		}
	}
	arn, ip, lastStatus, err := d.client.RunTask(ctx, driverConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start ECS task: %v", err)
	}

	if lastStatus != ecsTaskStatusRunning && lastStatus != ecsTaskStatusStopped {
		startCh := make(chan *startResult)
		go d.handleStart(ctx, arn, startCh)

		select {
		case <-ctx.Done():
			newCtx, newCancel := context.WithTimeout(context.Background(), time.Minute)
			defer newCancel()
			_ = d.client.StopTask(newCtx, arn)
			return nil, nil, fmt.Errorf("failed to start ECS task: context done")

		case <-d.ctx.Done():
			newCtx, newCancel := context.WithTimeout(context.Background(), time.Minute)
			defer newCancel()
			_ = d.client.StopTask(newCtx, arn)
			return nil, nil, fmt.Errorf("failed to start ECS task: context done")

		case startRes := <-startCh:
			if startRes.err != nil {
				newCtx, newCancel := context.WithTimeout(context.Background(), time.Minute)
				defer newCancel()
				_ = d.client.StopTask(newCtx, arn)
				return nil, nil, startRes.err
			}
			ip = startRes.ip
			lastStatus = startRes.status
		}
	}

	var net *drivers.DriverNetwork
	if ip != "" && driverConfig.Advertise {
		net = &drivers.DriverNetwork{
			PortMap:       driverConfig.PortMap,
			IP:            ip,
			AutoAdvertise: true,
		}
	}

	driverState := TaskState{
		TaskConfig:    cfg,
		StartedAt:     time.Now(),
		ARN:           arn,
		DriverNetwork: net,
	}

	h := newTaskHandle(d.logger, driverState, cfg, d.client, net)

	if err := handle.SetDriverState(&driverState); err != nil {
		d.logger.Error("failed to start task, error setting driver state", "error", err)
		h.stop(false)
		return nil, nil, fmt.Errorf("failed to set driver state: %v", err)
	}

	d.tasks.Set(cfg.ID, h)

	go h.run()

	return handle, net, nil
}

func (d *Driver) handleStart(ctx context.Context, arn string, startCh chan *startResult) {
	defer close(startCh)
	var err error
	var lastStatus, ip string
	for lastStatus != ecsTaskStatusRunning && lastStatus != ecsTaskStatusStopped {
		select {
		case <-ctx.Done():
			return

		case <-d.ctx.Done():
			return

		case <-time.After(10 * time.Second):
		}
		lastStatus, ip, err = d.client.DescribeTaskStatus(ctx, arn)
		if err != nil {
			d.logger.Warn("ecs describe task", "err", err.Error())
			startCh <- &startResult{
				err: err,
			}
			return
		}
	}
	startCh <- &startResult{
		ip:     ip,
		status: lastStatus,
	}
}

func (d *Driver) WaitTask(ctx context.Context, taskID string) (<-chan *drivers.ExitResult, error) {
	d.logger.Info("WaitTask() called", "task_id", taskID)
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	ch := make(chan *drivers.ExitResult)
	go d.handleWait(ctx, handle, ch)

	return ch, nil
}

func (d *Driver) handleWait(ctx context.Context, handle *taskHandle, ch chan *drivers.ExitResult) {
	defer close(ch)

	var result *drivers.ExitResult
	select {
	case <-ctx.Done():
		return
	case <-d.ctx.Done():
		return
	case <-handle.doneCh:
		result = &drivers.ExitResult{
			ExitCode: handle.exitResult.ExitCode,
			Signal:   handle.exitResult.Signal,
			Err:      nil,
		}
	}

	select {
	case <-ctx.Done():
		return
	case <-d.ctx.Done():
		return
	case ch <- result:
	}
}

func (d *Driver) StopTask(taskID string, timeout time.Duration, signal string) error {
	d.logger.Info("stopping ecs task", "task_id", taskID, "timeout", timeout, "signal", signal)
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	// Detach is that's the signal, otherwise kill
	detach := signal == drivers.DetachSignal
	handle.stop(detach)

	// Wait for handle to finish
	select {
	case <-handle.doneCh:
	case <-time.After(timeout):
		return fmt.Errorf("timed out waiting for ecs task (id=%s) to stop (detach=%t)",
			taskID, detach)
	}

	d.logger.Info("ecs task stopped", "task_id", taskID, "timeout", timeout,
		"signal", signal)
	return nil
}

func (d *Driver) DestroyTask(taskID string, force bool) error {
	d.logger.Info("destroying ecs task", "task_id", taskID, "force", force)
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	if handle.IsRunning() && !force {
		return fmt.Errorf("cannot destroy running task")
	}

	// Safe to always kill here as detaching will have already happened
	handle.stop(false)

	d.tasks.Delete(taskID)
	d.logger.Info("ecs task destroyed", "task_id", taskID, "force", force)
	return nil
}

func (d *Driver) InspectTask(taskID string) (*drivers.TaskStatus, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}
	return handle.TaskStatus(), nil
}

func (d *Driver) TaskStats(ctx context.Context, taskID string, interval time.Duration) (<-chan *structs.TaskResourceUsage, error) {
	d.logger.Info("sending ecs task stats", "task_id", taskID)
	_, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	ch := make(chan *drivers.TaskResourceUsage)

	go func() {
		defer d.logger.Info("stopped sending ecs task stats", "task_id", taskID)
		defer close(ch)
		for {
			select {
			case <-time.After(interval):

				// Nomad core does not currently have any resource based
				// support for remote drivers. Once this changes, we may be
				// able to report actual usage here.
				//
				// This is required, otherwise the driver panics.
				ch <- &structs.TaskResourceUsage{
					ResourceUsage: &drivers.ResourceUsage{
						MemoryStats: &drivers.MemoryStats{},
						CpuStats:    &drivers.CpuStats{},
					},
					Timestamp: time.Now().UTC().UnixNano(),
				}
			case <-ctx.Done():
				return
			}

		}
	}()

	return ch, nil
}

func (d *Driver) TaskEvents(ctx context.Context) (<-chan *drivers.TaskEvent, error) {
	d.logger.Info("retrieving task events")
	return d.eventer.TaskEvents(ctx)
}

func (d *Driver) SignalTask(_ string, _ string) error {
	return fmt.Errorf("ECS driver does not support signals")
}

func (d *Driver) ExecTask(_ string, _ []string, _ time.Duration) (*drivers.ExecTaskResult, error) {
	return nil, fmt.Errorf("ECS driver does not support exec")
}
