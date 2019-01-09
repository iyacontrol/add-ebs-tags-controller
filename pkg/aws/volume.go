package aws

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"net/url"
	"regexp"
	"strings"
)


// awsVolumeRegMatch represents Regex Match for AWS volume.
var awsVolumeRegMatch = regexp.MustCompile("^vol-[^/]*$")


// EBSVolumeID represents the ID of the volume in the AWS API, e.g.
// vol-12345678 The "traditional" format is "vol-12345678" A new longer format
// is also being introduced: "vol-12345678abcdef01" We should not assume
// anything about the length or format, though it seems reasonable to assume
// that volumes will continue to start with "vol-".
type EBSVolumeID string

func (i EBSVolumeID) awsString() *string {
	return aws.String(string(i))
}

// KubernetesVolumeID represents the id for a volume in the kubernetes API;
// a few forms are recognized:
//  * aws://<zone>/<awsVolumeId>
//  * aws:///<awsVolumeId>
//  * <awsVolumeId>
type KubernetesVolumeID string


// MapToAWSVolumeID extracts the EBSVolumeID from the KubernetesVolumeID
func (name KubernetesVolumeID) MapToAWSVolumeID() (EBSVolumeID, error) {
	// name looks like aws://availability-zone/awsVolumeId

	// The original idea of the URL-style name was to put the AZ into the
	// host, so we could find the AZ immediately from the name without
	// querying the API.  But it turns out we don't actually need it for
	// multi-AZ clusters, as we put the AZ into the labels on the PV instead.
	// However, if in future we want to support multi-AZ cluster
	// volume-awareness without using PersistentVolumes, we likely will
	// want the AZ in the host.

	s := string(name)

	if !strings.HasPrefix(s, "aws://") {
		// Assume a bare aws volume id (vol-1234...)
		// Build a URL with an empty host (AZ)
		s = "aws://" + "" + "/" + s
	}
	url, err := url.Parse(s)
	if err != nil {
		// TODO: Maybe we should pass a URL into the Volume functions
		return "", fmt.Errorf("Invalid disk name (%s): %v", name, err)
	}
	if url.Scheme != "aws" {
		return "", fmt.Errorf("Invalid scheme for AWS volume (%s)", name)
	}

	awsID := url.Path
	awsID = strings.Trim(awsID, "/")

	// We sanity check the resulting volume; the two known formats are
	// vol-12345678 and vol-12345678abcdef01
	if !awsVolumeRegMatch.MatchString(awsID) {
		return "", fmt.Errorf("Invalid format for AWS volume (%s)", name)
	}

	return EBSVolumeID(awsID), nil
}

// GetAWSVolumeID converts a Kubernetes volume ID to an AWS volume ID
func GetAWSVolumeID(kubeVolumeID string) (string, error) {
	kid := KubernetesVolumeID(kubeVolumeID)
	awsID, err := kid.MapToAWSVolumeID()
	return string(awsID), err
}