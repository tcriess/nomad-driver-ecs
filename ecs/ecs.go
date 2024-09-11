package ecs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/smithy-go/ptr"
	"github.com/hashicorp/go-hclog"
)

// ecsClientInterface encapsulates all the required AWS functionality to
// successfully run tasks via this plugin.
type ecsClientInterface interface {

	// DescribeCluster is used to determine the health of the plugin by
	// querying AWS for the cluster and checking its current status. A status
	// other than ACTIVE is considered unhealthy.
	DescribeCluster(ctx context.Context) error

	// DescribeTaskStatus attempts to return the current health status of the
	// ECS task and the IP address and should be used for health checking.
	// It also returns the stop and exit code, if applicable
	DescribeTaskStatus(ctx context.Context, taskARN string) (string, string, string, int, error)

	// RunTask is used to trigger the running of a new ECS task based on the
	// provided configuration. The ARN of the task, the IP address, run status,
	// as well as any errors are returned to the caller.
	RunTask(ctx context.Context, cfg TaskConfig) (string, string, string, error)

	// StopTask stops the running ECS task, adding a custom message which can
	// be viewed via the AWS console specifying it was this Nomad driver which
	// performed the action.
	StopTask(ctx context.Context, taskARN string) error

	// RegisterTaskDefinition creates a new task definition, which can then be
	// used to run a task.
	RegisterTaskDefinition(ctx context.Context, family string, containerDefinitions []ecstypes.ContainerDefinition, cpu string, memory string, ephemeralStorage int32, executionRoleArn string, taskRoleArn string) (int32, error)

	// CheckTaskDefinition checks if the task definition is the same as the one in AWS
	CheckTaskDefinition(ctx context.Context, family string, containerDefinitions []ecstypes.ContainerDefinition, cpu string, memory string, ephemeralStorage int32, executionRoleArn string, taskRoleArn string) (bool, error)

	// CreateLogGroup creates a new log group (if it does not exist already)
	CreateLogGroup(ctx context.Context, logGroupName string) error
}

type awsEcsClient struct {
	cluster   string
	ecsClient *ecs.Client
	cwClient  *cloudwatchlogs.Client
	logger    hclog.Logger
}

// DescribeCluster satisfies the ecs.ecsClientInterface DescribeCluster
// interface function.
func (c awsEcsClient) DescribeCluster(ctx context.Context) error {
	input := ecs.DescribeClustersInput{Clusters: []string{c.cluster}}

	resp, err := c.ecsClient.DescribeClusters(ctx, &input)
	if err != nil {
		return err
	}

	if len(resp.Clusters) > 1 || len(resp.Clusters) < 1 {
		return fmt.Errorf("AWS returned %v ECS clusters, expected 1", len(resp.Clusters))
	}

	if *resp.Clusters[0].Status != "ACTIVE" {
		return fmt.Errorf("ECS cluster status: %s", *resp.Clusters[0].Status)
	}

	return nil
}

// DescribeTaskStatus satisfies the ecs.ecsClientInterface DescribeTaskStatus
// interface function.
func (c awsEcsClient) DescribeTaskStatus(ctx context.Context, taskARN string) (string, string, string, int, error) {
	input := ecs.DescribeTasksInput{
		Cluster: aws.String(c.cluster),
		Tasks:   []string{taskARN},
	}

	resp, err := c.ecsClient.DescribeTasks(ctx, &input)
	if err != nil {
		return "", "", "", 0, err
	}
	ip := ""
	if len(resp.Tasks[0].Containers) > 0 && len(resp.Tasks[0].Containers[0].NetworkInterfaces) > 0 {
		ip = ptr.ToString(resp.Tasks[0].Containers[0].NetworkInterfaces[0].PrivateIpv4Address)
	}
	exitCode := 0
	if len(resp.Tasks[0].Containers) > 0 {
		exitCode = int(aws.ToInt32(resp.Tasks[0].Containers[0].ExitCode))
	}
	stopCode := string(resp.Tasks[0].StopCode)
	return *resp.Tasks[0].LastStatus, ip, stopCode, exitCode, nil
}

// RunTask satisfies the ecs.ecsClientInterface RunTask interface function.
func (c awsEcsClient) RunTask(ctx context.Context, cfg TaskConfig) (string, string, string, error) {
	input := c.buildTaskInput(cfg)

	/*
		if err := input.Validate(); err != nil {
			return "", "", "", fmt.Errorf("failed to validate: %w", err)
		}
	*/

	resp, err := c.ecsClient.RunTask(ctx, input)
	if err != nil {
		return "", "", "", err
	}
	var lastStatus string
	var ip string
	lastStatus = ptr.ToString(resp.Tasks[0].LastStatus)
	if len(resp.Tasks[0].Containers) > 0 && len(resp.Tasks[0].Containers[0].NetworkInterfaces) > 0 {
		ip = ptr.ToString(resp.Tasks[0].Containers[0].NetworkInterfaces[0].PrivateIpv4Address)
	}
	return *resp.Tasks[0].TaskArn, ip, lastStatus, nil
}

// buildTaskInput is used to convert the jobspec supplied configuration input
// into the appropriate ecs.RunTaskInput object.
func (c awsEcsClient) buildTaskInput(cfg TaskConfig) *ecs.RunTaskInput {
	input := ecs.RunTaskInput{
		Cluster:              aws.String(c.cluster),
		Count:                aws.Int32(1),
		StartedBy:            aws.String("nomad-ecs-driver"),
		NetworkConfiguration: &ecstypes.NetworkConfiguration{AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{}},
	}

	if len(cfg.Task.Tags) > 0 {
		tags := []ecstypes.Tag{}
		for _, tag := range cfg.Task.Tags {
			tags = append(tags, ecstypes.Tag{
				Key:   aws.String(tag.Key),
				Value: aws.String(tag.Value),
			})
		}
		input.Tags = tags
	}

	if cfg.Task.LaunchType != "" {
		if cfg.Task.LaunchType == "EC2" {
			input.LaunchType = ecstypes.LaunchTypeEc2
		} else if cfg.Task.LaunchType == "FARGATE" {
			input.LaunchType = ecstypes.LaunchTypeFargate
		}
	}

	if cfg.Task.TaskDefinition != "" {
		input.TaskDefinition = aws.String(cfg.Task.TaskDefinition)
	}

	// Handle the task networking setup.
	if cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.AssignPublicIP != "" {
		assignPublicIp := cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.AssignPublicIP
		if assignPublicIp == "ENABLED" {
			input.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp = ecstypes.AssignPublicIpEnabled
		} else if assignPublicIp == "DISABLED" {
			input.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp = ecstypes.AssignPublicIpDisabled
		}
	}
	if len(cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.SecurityGroups) > 0 {
		input.NetworkConfiguration.AwsvpcConfiguration.SecurityGroups = cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.SecurityGroups
	}
	if len(cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.Subnets) > 0 {
		input.NetworkConfiguration.AwsvpcConfiguration.Subnets = cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.Subnets
	}

	return &input
}

// StopTask satisfies the ecs.ecsClientInterface StopTask interface function.
func (c awsEcsClient) StopTask(ctx context.Context, taskARN string) error {
	input := ecs.StopTaskInput{
		Cluster: aws.String(c.cluster),
		Task:    &taskARN,
		Reason:  aws.String("stopped by nomad-ecs-driver automation"),
	}

	_, err := c.ecsClient.StopTask(ctx, &input)
	return err
}

func checkSlice[t comparable](arr1, arr2 []t, order bool) (same bool) {
	if len(arr1) != len(arr2) {
		return false
	}
	if order {
		for i, el1 := range arr1 {
			if el1 != arr2[i] {
				return false
			}
		}
	} else {
		for _, el1 := range arr1 {
			found := false
			for _, el2 := range arr2 {
				if el1 == el2 {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func checkKVSlice(arr1, arr2 []ecstypes.KeyValuePair, order bool) (same bool) {
	if len(arr1) != len(arr2) {
		return false
	}
	if order {
		for i, el1 := range arr1 {
			if aws.ToString(el1.Name) != aws.ToString(arr2[i].Name) || aws.ToString(el1.Value) != aws.ToString(arr2[i].Value) {
				return false
			}
		}
	} else {
		for _, el1 := range arr1 {
			found := false
			for _, el2 := range arr2 {
				if aws.ToString(el1.Name) == aws.ToString(el2.Name) && aws.ToString(el1.Value) == aws.ToString(el2.Value) {
					found = true
					break
				}
			}
			if !found {

				return false
			}
		}
	}
	return true
}

func checkPMSlice(arr1, arr2 []ecstypes.PortMapping, order bool) (same bool) {
	if len(arr1) != len(arr2) {
		return false
	}
	if order {
		for i, el1 := range arr1 {
			if el1.Protocol != arr2[i].Protocol || aws.ToInt32(el1.HostPort) != aws.ToInt32(arr2[i].HostPort) || aws.ToInt32(el1.ContainerPort) != aws.ToInt32(arr2[i].ContainerPort) {
				return false
			}
		}
	} else {
		for _, el1 := range arr1 {
			found := false
			for _, el2 := range arr2 {
				if el1.Protocol == el2.Protocol && aws.ToInt32(el1.HostPort) == aws.ToInt32(el2.HostPort) && aws.ToInt32(el1.ContainerPort) == aws.ToInt32(el2.ContainerPort) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

// CheckTaskDefinition checks if the task definition matches the already registered task definition
func (c awsEcsClient) CheckTaskDefinition(ctx context.Context, family string, containerDefinitions []ecstypes.ContainerDefinition, cpu string, memory string, ephemeralStorage int32, executionRoleArn string, taskRoleArn string) (bool, error) {
	listInput := ecs.ListTaskDefinitionFamiliesInput{
		FamilyPrefix: aws.String(family),
	}
	listResp, err := c.ecsClient.ListTaskDefinitionFamilies(ctx, &listInput)
	if err != nil {
		return false, err
	}
	c.logger.Info("ListTaskDefinitionFamilies", "resp", listResp)
	if len(listResp.Families) == 0 {
		return false, nil
	}
	input := ecs.DescribeTaskDefinitionInput{
		// Include:        nil,
		TaskDefinition: aws.String(family), // family (latest active), family:revision or full arn
	}
	resp, err := c.ecsClient.DescribeTaskDefinition(ctx, &input)
	if err != nil {
		return false, err
	}

	isSame := true
	if aws.ToString(resp.TaskDefinition.Cpu) != cpu {
		c.logger.Info("diff cpu", "cpu", cpu, "resp cpu", aws.ToString(resp.TaskDefinition.Cpu))
		isSame = false
	}
	if aws.ToString(resp.TaskDefinition.Memory) != memory {
		c.logger.Info("diff memory", "memory", memory, "resp memory", aws.ToString(resp.TaskDefinition.Memory))
		isSame = false
	}
	if resp.TaskDefinition.EphemeralStorage != nil {
		if resp.TaskDefinition.EphemeralStorage.SizeInGiB != ephemeralStorage {
			c.logger.Info("diff ephemeral storage", "ephemeral storage", ephemeralStorage, "resp memory", resp.TaskDefinition.EphemeralStorage.SizeInGiB)
			isSame = false
		}
	} else {
		if ephemeralStorage != 0 {
			c.logger.Info("diff ephemeral storage", "ephemeral storage", ephemeralStorage, "resp memory", 0)
			isSame = false
		}
	}
	if aws.ToString(resp.TaskDefinition.ExecutionRoleArn) != executionRoleArn {
		c.logger.Info("diff exec role", "exec role", executionRoleArn, "resp exec role", aws.ToString(resp.TaskDefinition.ExecutionRoleArn))
		isSame = false
	}
	if aws.ToString(resp.TaskDefinition.TaskRoleArn) != taskRoleArn {
		c.logger.Info("diff task role", "task role", taskRoleArn, "resp task role", aws.ToString(resp.TaskDefinition.TaskRoleArn))
		isSame = false
	}
	for _, cd := range containerDefinitions {
		found := false
		for _, existingCd := range resp.TaskDefinition.ContainerDefinitions {
			c.logger.Info("checking", "existing cd", existingCd)
			if existingCd.Cpu == cd.Cpu && aws.ToInt32(existingCd.Memory) == aws.ToInt32(cd.Memory) && checkSlice(existingCd.Command, cd.Command, true) && checkSlice(existingCd.EntryPoint, cd.EntryPoint, true) && checkKVSlice(existingCd.Environment, cd.Environment, false) && aws.ToString(existingCd.Image) == aws.ToString(cd.Image) && checkPMSlice(existingCd.PortMappings, cd.PortMappings, false) {
				if existingCd.LogConfiguration != nil && cd.LogConfiguration != nil {
					if existingCd.LogConfiguration.LogDriver == cd.LogConfiguration.LogDriver {
						found = true
						break
					}
				} else {
					if existingCd.LogConfiguration == nil && cd.LogConfiguration == nil {
						found = true
						break
					}
				}
			}
			c.logger.Info("differences", "command", checkSlice(existingCd.Command, cd.Command, true), "entrypoint", checkSlice(existingCd.EntryPoint, cd.EntryPoint, true), "env", checkKVSlice(existingCd.Environment, cd.Environment, false), "pm", checkPMSlice(existingCd.PortMappings, cd.PortMappings, false))
		}
		if !found {
			c.logger.Info("container definition not found", "cd", cd)
			return false, nil
		}
	}
	return isSame, nil
}

func (c awsEcsClient) RegisterTaskDefinition(ctx context.Context, family string, containerDefinitions []ecstypes.ContainerDefinition, cpu string, memory string, ephemeralStorage int32, executionRoleArn string, taskRoleArn string) (int32, error) {
	var eph *ecstypes.EphemeralStorage
	if ephemeralStorage > 0 {
		eph = &ecstypes.EphemeralStorage{
			SizeInGiB: ephemeralStorage,
		}
	}
	input := ecs.RegisterTaskDefinitionInput{
		ContainerDefinitions: containerDefinitions,
		Cpu:                  aws.String(cpu),
		ExecutionRoleArn:     aws.String(executionRoleArn),
		Family:               aws.String(family),
		EphemeralStorage:     eph,
		// InferenceAccelerators:   nil,
		// IpcMode:                 "",
		Memory:      aws.String(memory),
		NetworkMode: "awsvpc",
		// PidMode:                 "",
		// PlacementConstraints:    nil,
		// ProxyConfiguration:      nil,
		// RequiresCompatibilities: nil,
		Tags: nil,
		// Volumes:                 nil,
	}
	if taskRoleArn != "" {
		input.TaskRoleArn = aws.String(taskRoleArn)
	}
	resp, err := c.ecsClient.RegisterTaskDefinition(ctx, &input)
	if err != nil {
		return 0, err
	}
	return resp.TaskDefinition.Revision, nil
}

func (c awsEcsClient) CreateLogGroup(ctx context.Context, logGroupName string) error {
	input := cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(logGroupName),
	}
	cont := true
	for cont {
		resp, err := c.cwClient.DescribeLogGroups(ctx, &input)
		if err != nil {
			return err
		}
		for _, lg := range resp.LogGroups {
			if aws.ToString(lg.LogGroupName) == logGroupName {
				return nil
			}
		}
		cont = aws.ToString(resp.NextToken) != ""
		input.NextToken = aws.String(aws.ToString(resp.NextToken))
	}

	createInput := cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(logGroupName),
	}
	_, err := c.cwClient.CreateLogGroup(ctx, &createInput)
	if err != nil {
		return err
	}

	return nil
}
