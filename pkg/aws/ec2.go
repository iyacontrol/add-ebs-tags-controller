package aws

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/resourcegroupstaggingapi"
	"github.com/ticketmaster/aws-sdk-go-cache/cache"
)

// EC2 is an abstraction over AWS', to allow mocking/other implementations
// Note that the DescribeX functions return a list, so callers don't need to deal with paging
// TODO: Should we rename this to AWS (EBS & ELB are not technically part of EC2)
type EC2 interface {
	CreateTags(resourceID string, additionalTags map[string]string) error
}

// awsSdkEC2 is an implementation of the EC2 interface, backed by aws-sdk-go
type awsSdkEC2 struct {
	ec2 *ec2.EC2
}

func Compute() (EC2, error) {
	cc := cache.NewConfig(5 * time.Minute)
	sess := newSession(&aws.Config{MaxRetries: aws.Int(3)}, false, cc)

	metadata := ec2metadata.New(sess)
	region, err := metadata.Region()
	if err != nil {
		return nil, fmt.Errorf("EC2 SD configuration requires a region")
	}

	awsConfig := &aws.Config{
		Region: &region,
	}

	service := ec2.New(sess, awsConfig)
	ec2 := &awsSdkEC2{
		ec2: service,
	}
	return ec2, nil
}

// CreateTags calls EC2 CreateTags, but adds retry-on-failure logic
// We retry mainly because if we create an object, we cannot tag it until it is "fully created" (eventual consistency)
// The error code varies though (depending on what we are tagging), so we simply retry on all errors
func (a *awsSdkEC2) CreateTags(resourceID string, additionalTags map[string]string) error {
	var awsTags []*ec2.Tag
	for k, v := range additionalTags {
		tag := &ec2.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}
		awsTags = append(awsTags, tag)
	}

	backoff := wait.Backoff{
		Duration: createTagInitialDelay,
		Factor:   createTagFactor,
		Steps:    createTagSteps,
	}
	request := &ec2.CreateTagsInput{}
	request.Resources = []*string{&resourceID}
	request.Tags = awsTags

	var lastErr error
	err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		_, err := a.ec2.CreateTags(request)
		if err == nil {
			return true, nil
		}

		// We could check that the error is retryable, but the error code changes based on what we are tagging
		// SecurityGroup: InvalidGroup.NotFound
		glog.V(2).Infof("Failed to create tags; will retry.  Error was %q", err)
		lastErr = err
		return false, nil
	})
	if err == wait.ErrWaitTimeout {
		// return real CreateTags error instead of timeout
		err = lastErr
	}
	return err
}

// newSession returns an AWS session based off of the provided AWS config
func newSession(awsconfig *aws.Config, AWSDebug bool, cc *cache.Config) *session.Session {
	session, err := session.NewSession(awsconfig)
	if err != nil {
		glog.ErrorDepth(4, fmt.Sprintf("Failed to create AWS session: %s", err.Error()))
		return nil
	}

	// Adds caching to session
	cache.AddCaching(session, cc)
	cc.SetCacheTTL(resourcegroupstaggingapi.ServiceName, "GetResources", time.Hour)
	cc.SetCacheTTL(ec2.ServiceName, "DescribeInstanceStatus", time.Minute)

	session.Handlers.Send.PushFront(func(r *request.Request) {
		if AWSDebug {
			glog.InfoDepth(4, fmt.Sprintf("Request: %s/%s, Payload: %s", r.ClientInfo.ServiceName, r.Operation.Name, r.Params))
		}
	})

	session.Handlers.Complete.PushFront(func(r *request.Request) {
		if r.Error != nil {
			if AWSDebug {
				glog.ErrorDepth(4, fmt.Sprintf("Failed request: %s/%s, Payload: %s, Error: %s", r.ClientInfo.ServiceName, r.Operation.Name, r.Params, r.Error))
			}
		} else {
			if AWSDebug {
				glog.InfoDepth(4, fmt.Sprintf("Response: %s/%s, Body: %s", r.ClientInfo.ServiceName, r.Operation.Name, r.Data))
			}
		}
	})
	return session
}
