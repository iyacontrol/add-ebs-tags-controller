package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"k8s.io/add-ebs-tags-controller/pkg/aws"
)

const controllerAgentName = "add-ebs-tags-controller"

// VolumeClaimAnnotationBlockStorageAdditionalTags is the annotation used on the volume
// claim to specify a comma-separated list of key-value pairs which will be recorded as
// additional tags in the EBS.
// For example: "Key1=Val1,Key2=Val2,KeyNoVal1=,KeyNoVal2"
const VolumeClaimAnnotationBlockStorageAdditionalTags = "volume.beta.kubernetes.io/aws-block-storage-additional-resource-tags"

// Controller is the controller implementation for pvc resources
type Controller struct {
	kubeclientset kubernetes.Interface

	volumeLister       corelisters.PersistentVolumeLister
	volumeListerSynced cache.InformerSynced

	claimLister       corelisters.PersistentVolumeClaimLister
	claimListerSynced cache.InformerSynced

	queue    workqueue.RateLimitingInterface
	recorder record.EventRecorder

	ec2 aws.EC2
}

// NewController returns a new instance of a controller
func NewController(kubeclientset kubernetes.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory) (*Controller, error) {

	claimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	volumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()

	glog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	ec2, err := aws.Compute()
	if err != nil {
		return nil, err
	}

	c := &Controller{
		kubeclientset: kubeclientset,

		volumeLister:       volumeInformer.Lister(),
		volumeListerSynced: volumeInformer.Informer().HasSynced,

		claimLister:       claimInformer.Lister(),
		claimListerSynced: claimInformer.Informer().HasSynced,

		queue:    workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "pvcs"),
		recorder: recorder,

		ec2: ec2,
	}

	glog.Info("Setting up event handlers")
	// Set up an event handler for when EventProvider resources change
	claimInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			glog.Infof("AddFunc called with object: %v", obj)
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				c.queue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			glog.Infof("UpdateFunc called with object: %v", new)
			o := old.(*corev1.PersistentVolumeClaim)
			n := new.(*corev1.PersistentVolumeClaim)

			if o.ResourceVersion == n.ResourceVersion {
				// Periodic resync will send update events for all known Objects.
				// Two different versions of the same Objects will always have different RVs.
				return
			}
			if o.Annotations[VolumeClaimAnnotationBlockStorageAdditionalTags] == n.Annotations[VolumeClaimAnnotationBlockStorageAdditionalTags] {
				return
			}

			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				c.queue.Add(key)
			}

		},
		DeleteFunc: func(obj interface{}) {
		},
	})

	return c, nil

}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	glog.Info("Starting add ebs tags controller")

	// Wait for the caches to be synced before starting workers
	glog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.claimListerSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("Starting workers")
	// Launch two workers to process the resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, 30*time.Second, stopCh)
	}

	glog.Info("Started workers")
	<-stopCh
	glog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.queue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.queue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.queue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Foo resource to be synced.
		if err := c.syncHandler(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.queue.Forget(obj)
		glog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two
func (c *Controller) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		glog.Errorf("invalid resource key: %s", key)
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	glog.Infof("\nReceived:  name: %v\n", name)

	claim, err := c.claimLister.PersistentVolumeClaims(namespace).Get(name)
	if err != nil {
		glog.Errorf("error getting pvc: %v", err)
		return fmt.Errorf("error getting pvc: %v", err)
	}
	glog.Infof("claim: %v", claim)

	if claim.Spec.VolumeName == "" {
		glog.Errorf("error  pv is not been boud to pvc : %s", name)
		return fmt.Errorf("error  pv is not been boud to pvc : %s", name)
	}

	volumeName := claim.Spec.VolumeName

	volume, err := c.volumeLister.Get(volumeName)
	if err != nil {
		glog.Errorf("error getting pv: %v", err)
		return fmt.Errorf("error getting pv: %v", err)
	}

	if tags := getVolumeClaimAnnotationBlockStorageAdditionalTags(claim.Annotations); len(tags) > 0 {
		// aws://ap-southeast-1b/vol-0c37a53ced37b40c3
		awsVolumeID, err := aws.GetAWSVolumeID(volume.Spec.AWSElasticBlockStore.VolumeID)
		if err != nil {
			glog.Errorf("error get aws volume id: %v", err)
			return fmt.Errorf("error get aws volume id: %v", err)
		}

		if err := c.ec2.CreateTags(awsVolumeID, tags); err != nil {
			glog.Errorf("error create tags for volume %s : %v", awsVolumeID, err)
			return fmt.Errorf("error create tags for volume %s : %v", awsVolumeID, err)
		}

		glog.Infof("success tag vloume, name: %v\n", name)

	} else {
		glog.Infof("the pvc not include tags, skip:  name: %v\n", name)
	}

	return nil
}

func getVolumeClaimAnnotationBlockStorageAdditionalTags(annotations map[string]string) map[string]string {
	additionalTags := make(map[string]string)
	if additionalTagsList, ok := annotations[VolumeClaimAnnotationBlockStorageAdditionalTags]; ok {
		additionalTagsList = strings.TrimSpace(additionalTagsList)

		// Break up list of "Key1=Val,Key2=Val2"
		tagList := strings.Split(additionalTagsList, ",")

		// Break up "Key=Val"
		for _, tagSet := range tagList {
			tag := strings.Split(strings.TrimSpace(tagSet), "=")

			// Accept "Key=val" or "Key=" or just "Key"
			if len(tag) >= 2 && len(tag[0]) != 0 {
				// There is a key and a value, so save it
				additionalTags[tag[0]] = tag[1]
			} else if len(tag) == 1 && len(tag[0]) != 0 {
				// Just "Key"
				additionalTags[tag[0]] = ""
			}
		}
	}

	return additionalTags
}
