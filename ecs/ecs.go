package ecs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/smithy-go/ptr"
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
	DescribeTaskStatus(ctx context.Context, taskARN string) (string, string, error)

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
	RegisterTaskDefinition(ctx context.Context, family string, containerDefinitions []ecs.ContainerDefinition, cpu string, memory string, executionRoleArn string, taskRoleArn string) (int64, error)

	// CheckTaskDefinition checks if the task definition is the same as the one in AWS
	CheckTaskDefinition(ctx context.Context, family string, containerDefinitions []ecs.ContainerDefinition, cpu string, memory string, executionRoleArn string, taskRoleArn string) (bool, error)
}

type awsEcsClient struct {
	cluster   string
	ecsClient *ecs.Client
}

// DescribeCluster satisfies the ecs.ecsClientInterface DescribeCluster
// interface function.
func (c awsEcsClient) DescribeCluster(ctx context.Context) error {
	input := ecs.DescribeClustersInput{Clusters: []string{c.cluster}}

	resp, err := c.ecsClient.DescribeClustersRequest(&input).Send(ctx)
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
func (c awsEcsClient) DescribeTaskStatus(ctx context.Context, taskARN string) (string, string, error) {
	input := ecs.DescribeTasksInput{
		Cluster: aws.String(c.cluster),
		Tasks:   []string{taskARN},
	}

	resp, err := c.ecsClient.DescribeTasksRequest(&input).Send(ctx)
	if err != nil {
		return "", "", err
	}
	ip := ""
	if len(resp.Tasks[0].Containers) > 0 && len(resp.Tasks[0].Containers[0].NetworkInterfaces) > 0 {
		ip = ptr.ToString(resp.Tasks[0].Containers[0].NetworkInterfaces[0].PrivateIpv4Address)
	}
	return *resp.Tasks[0].LastStatus, ip, nil
}

// RunTask satisfies the ecs.ecsClientInterface RunTask interface function.
func (c awsEcsClient) RunTask(ctx context.Context, cfg TaskConfig) (string, string, string, error) {
	input := c.buildTaskInput(cfg)

	if err := input.Validate(); err != nil {
		return "", "", "", fmt.Errorf("failed to validate: %w", err)
	}

	resp, err := c.ecsClient.RunTaskRequest(input).Send(ctx)
	if err != nil {
		return "", "", "", err
	}
	var lastStatus string
	var ip string
	lastStatus = ptr.ToString(resp.RunTaskOutput.Tasks[0].LastStatus)
	if len(resp.RunTaskOutput.Tasks[0].Containers) > 0 && len(resp.RunTaskOutput.Tasks[0].Containers[0].NetworkInterfaces) > 0 {
		ip = ptr.ToString(resp.RunTaskOutput.Tasks[0].Containers[0].NetworkInterfaces[0].PrivateIpv4Address)
	}
	return *resp.RunTaskOutput.Tasks[0].TaskArn, ip, lastStatus, nil
}

// buildTaskInput is used to convert the jobspec supplied configuration input
// into the appropriate ecs.RunTaskInput object.
func (c awsEcsClient) buildTaskInput(cfg TaskConfig) *ecs.RunTaskInput {
	input := ecs.RunTaskInput{
		Cluster:              aws.String(c.cluster),
		Count:                aws.Int64(1),
		StartedBy:            aws.String("nomad-ecs-driver"),
		NetworkConfiguration: &ecs.NetworkConfiguration{AwsvpcConfiguration: &ecs.AwsVpcConfiguration{}},
	}

	if len(cfg.Task.Tags) > 0 {
		tags := []ecs.Tag{}
		for _, tag := range cfg.Task.Tags {
			tags = append(tags, ecs.Tag{
				Key:   aws.String(tag.Key),
				Value: aws.String(tag.Value),
			})
		}
		input.Tags = tags
	}

	if cfg.Task.LaunchType != "" {
		if cfg.Task.LaunchType == "EC2" {
			input.LaunchType = ecs.LaunchTypeEc2
		} else if cfg.Task.LaunchType == "FARGATE" {
			input.LaunchType = ecs.LaunchTypeFargate
		}
	}

	if cfg.Task.TaskDefinition != "" {
		input.TaskDefinition = aws.String(cfg.Task.TaskDefinition)
	}

	// Handle the task networking setup.
	if cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.AssignPublicIP != "" {
		assignPublicIp := cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.AssignPublicIP
		if assignPublicIp == "ENABLED" {
			input.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp = ecs.AssignPublicIpEnabled
		} else if assignPublicIp == "DISABLED" {
			input.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp = ecs.AssignPublicIpDisabled
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

	_, err := c.ecsClient.StopTaskRequest(&input).Send(ctx)
	return err
}

func NewContainerDefinition(command []string, entryPoint []string, env map[string]string, image string, memory int64, name string, tcpMap map[int64]int64, privileged bool, repositoryCredentials string, workingDirectory string) ecs.ContainerDefinition {
	environment := make([]ecs.KeyValuePair, 0, len(env))
	for k, v := range env {
		environment = append(environment, ecs.KeyValuePair{
			Name:  aws.String(k),
			Value: aws.String(v),
		})
	}
	portMappings := make([]ecs.PortMapping, 0, len(tcpMap))
	for k, v := range tcpMap {
		portMappings = append(portMappings, ecs.PortMapping{
			ContainerPort: aws.Int64(v),
			HostPort:      aws.Int64(k),
			Protocol:      "tcp",
		})
	}
	var repCred *ecs.RepositoryCredentials
	if repositoryCredentials != "" {
		repCred = &ecs.RepositoryCredentials{CredentialsParameter: aws.String(repositoryCredentials)}
	}
	var workDir *string
	if workingDirectory != "" {
		workDir = aws.String(workingDirectory)
	}
	return ecs.ContainerDefinition{
		Command: command,
		// Cpu:                    nil,
		// DependsOn:              nil,
		// DisableNetworking:      nil,
		// DnsSearchDomains:       nil,
		// DnsServers:             nil,
		// DockerLabels:           nil,
		// DockerSecurityOptions:  nil,
		EntryPoint:  entryPoint,
		Environment: environment,
		// Essential:              nil,
		// ExtraHosts:             nil,
		// FirelensConfiguration:  nil,
		// HealthCheck:            nil,
		// Hostname:               nil,
		Image: aws.String(image),
		// Interactive:            nil,
		// Links:                  nil,
		// LinuxParameters:        nil,
		// LogConfiguration:       nil,
		Memory: aws.Int64(memory),
		// MemoryReservation:      nil,
		// MountPoints:            nil,
		Name:         aws.String(name),
		PortMappings: portMappings,
		Privileged:   aws.Bool(privileged),
		// PseudoTerminal:         nil,
		// ReadonlyRootFilesystem: nil,
		RepositoryCredentials: repCred,
		// ResourceRequirements:   nil,
		// Secrets:                nil,
		// StartTimeout:           nil,
		// StopTimeout:            nil,
		// SystemControls:         nil,
		// Ulimits:                nil,
		// User:                   nil,
		// VolumesFrom:            nil,
		WorkingDirectory: workDir,
	}
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

func checkKVSlice(arr1, arr2 []ecs.KeyValuePair, order bool) (same bool) {
	if len(arr1) != len(arr2) {
		return false
	}
	if order {
		for i, el1 := range arr1 {
			if el1.Name != arr2[i].Name || aws.StringValue(el1.Value) != aws.StringValue(arr2[i].Value) {
				return false
			}
		}
	} else {
		for _, el1 := range arr1 {
			found := false
			for _, el2 := range arr2 {
				if el1.Name == el2.Name && aws.StringValue(el1.Value) == aws.StringValue(el2.Value) {
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

func checkPMSlice(arr1, arr2 []ecs.PortMapping, order bool) (same bool) {
	if len(arr1) != len(arr2) {
		return false
	}
	if order {
		for i, el1 := range arr1 {
			if el1.Protocol != arr2[i].Protocol || aws.Int64Value(el1.HostPort) != aws.Int64Value(arr2[i].HostPort) || aws.Int64Value(el1.ContainerPort) != aws.Int64Value(arr2[i].ContainerPort) {
				return false
			}
		}
	} else {
		for _, el1 := range arr1 {
			found := false
			for _, el2 := range arr2 {
				if el1.Protocol == el2.Protocol && aws.Int64Value(el1.HostPort) == aws.Int64Value(el2.HostPort) && aws.Int64Value(el1.ContainerPort) == aws.Int64Value(el2.ContainerPort) {
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

func (c awsEcsClient) CheckTaskDefinition(ctx context.Context, family string, containerDefinitions []ecs.ContainerDefinition, cpu string, memory string, executionRoleArn string, taskRoleArn string) (bool, error) {
	listInput := ecs.ListTaskDefinitionFamiliesInput{
		FamilyPrefix: aws.String(family),
	}
	listResp, err := c.ecsClient.ListTaskDefinitionFamiliesRequest(&listInput).Send(ctx)
	if err != nil {
		return false, err
	}
	if len(listResp.ListTaskDefinitionFamiliesOutput.Families) == 0 {
		return false, nil
	}
	input := ecs.DescribeTaskDefinitionInput{
		// Include:        nil,
		TaskDefinition: aws.String(family), // family (latest active), family:revision or full arn
	}
	resp, err := c.ecsClient.DescribeTaskDefinitionRequest(&input).Send(ctx)
	if err != nil {
		return false, err
	}
	isSame := true
	if aws.StringValue(resp.DescribeTaskDefinitionOutput.TaskDefinition.Cpu) != cpu {
		isSame = false
	}
	if aws.StringValue(resp.DescribeTaskDefinitionOutput.TaskDefinition.Memory) != memory {
		isSame = false
	}
	if aws.StringValue(resp.DescribeTaskDefinitionOutput.TaskDefinition.ExecutionRoleArn) != executionRoleArn {
		isSame = false
	}
	if aws.StringValue(resp.DescribeTaskDefinitionOutput.TaskDefinition.TaskRoleArn) != taskRoleArn {
		isSame = false
	}
	for _, cd := range containerDefinitions {
		found := false
		for _, existingCd := range resp.DescribeTaskDefinitionOutput.TaskDefinition.ContainerDefinitions {
			if aws.Int64Value(existingCd.Cpu) == aws.Int64Value(cd.Cpu) && aws.Int64Value(existingCd.Memory) == aws.Int64Value(cd.Memory) && checkSlice(existingCd.Command, cd.Command, true) && checkSlice(existingCd.EntryPoint, cd.EntryPoint, true) && checkKVSlice(existingCd.Environment, cd.Environment, false) && aws.StringValue(existingCd.Image) == aws.StringValue(cd.Image) && checkPMSlice(existingCd.PortMappings, cd.PortMappings, false) {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	return isSame, nil
}

func (c awsEcsClient) RegisterTaskDefinition(ctx context.Context, family string, containerDefinitions []ecs.ContainerDefinition, cpu string, memory string, executionRoleArn string, taskRoleArn string) (int64, error) {
	input := ecs.RegisterTaskDefinitionInput{
		ContainerDefinitions: containerDefinitions,
		Cpu:                  aws.String(cpu),
		ExecutionRoleArn:     aws.String(executionRoleArn),
		Family:               aws.String(family),
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
	resp, err := c.ecsClient.RegisterTaskDefinitionRequest(&input).Send(ctx)
	if err != nil {
		return 0, err
	}
	return aws.Int64Value(resp.RegisterTaskDefinitionOutput.TaskDefinition.Revision), nil
}
