/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/dranet/pkg/apis"
	"github.com/google/dranet/pkg/inventory"
	"github.com/google/dranet/pkg/statestore"

	"github.com/containerd/nri/pkg/stub"
	"github.com/google/dranet/internal/nlwrap"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

const (
	kubeletPluginRegistryPath = "/var/lib/kubelet/plugins_registry"
	kubeletPluginPath         = "/var/lib/kubelet/plugins"
)

const (
	// maxAttempts indicates the number of times the driver will try to recover itself before failing
	maxAttempts = 5
)

// This interface is our internal contract for the behavior we need from a *kubeletplugin.Helper, created specifically so we can fake it in tests.
type pluginHelper interface {
	PublishResources(context.Context, resourceslice.DriverResources) error
	Stop()
	RegistrationStatus() *registerapi.RegistrationStatus
}

// This interface is our internal contract for the behavior we need from a *inventory.DB, created specifically so we can fake it in tests.
type inventoryDB interface {
	Run(context.Context) error
	GetResources(context.Context) <-chan []resourceapi.Device
	GetNetInterfaceName(string) (string, error)
	AddPodNetNs(podKey string, netNs string)
	RemovePodNetNs(podKey string)
	GetPodNetNs(podKey string) (netNs string)
}

// WithFilter
func WithFilter(filter cel.Program) Option {
	return func(o *NetworkDriver) {
		o.celProgram = filter
	}
}

// WithInventory sets the inventory database for the driver.
func WithInventory(db inventoryDB) Option {
	return func(o *NetworkDriver) {
		o.netdb = db
	}
}

// WithStateStore sets the state store for persisting driver state.
func WithStateStore(store statestore.Store) Option {
	return func(o *NetworkDriver) {
		o.stateStore = store
	}
}

// WithPodUID sets the pod UID for rolling updates support.
func WithPodUID(uid string) Option {
	return func(o *NetworkDriver) {
		o.podUID = uid
	}
}

type NetworkDriver struct {
	driverName string
	nodeName   string
	kubeClient kubernetes.Interface
	draPlugin  pluginHelper
	nriPlugin  stub.Stub
	podUID     string

	// contains the host interfaces
	netdb      inventoryDB
	celProgram cel.Program

	// stateStore for persisting state across restarts
	stateStore statestore.Store

	// Cache the rdma shared mode state
	rdmaSharedMode bool
	podConfigStore *PodConfigStore
}

type Option func(*NetworkDriver)

func Start(ctx context.Context, driverName string, kubeClient kubernetes.Interface, nodeName string, opts ...Option) (*NetworkDriver, error) {
	registerMetrics()

	rdmaNetnsMode, err := nlwrap.RdmaSystemGetNetnsMode()
	if err != nil {
		klog.Infof("failed to determine the RDMA subsystem's network namespace mode, assume shared mode: %v", err)
		rdmaNetnsMode = apis.RdmaNetnsModeShared
	} else {
		klog.Infof("RDMA subsystem in mode: %s", rdmaNetnsMode)
	}

	plugin := &NetworkDriver{
		driverName:     driverName,
		nodeName:       nodeName,
		kubeClient:     kubeClient,
		rdmaSharedMode: rdmaNetnsMode == apis.RdmaNetnsModeShared,
	}

	for _, o := range opts {
		o(plugin)
	}

	// Initialize pod config store with state store if provided
	if plugin.stateStore != nil {
		plugin.podConfigStore = NewPodConfigStoreWithBackend(plugin.stateStore)
		// Reconcile stored state with actual pods on this node
		if err := plugin.reconcileStateStore(ctx); err != nil {
			klog.Warningf("Failed to reconcile state store: %v", err)
		}
	} else {
		plugin.podConfigStore = NewPodConfigStore()
	}

	driverPluginPath := filepath.Join(kubeletPluginPath, driverName)
	err = os.MkdirAll(driverPluginPath, 0750)
	if err != nil {
		return nil, fmt.Errorf("failed to create plugin path %s: %v", driverPluginPath, err)
	}

	kubeletOpts := []kubeletplugin.Option{
		kubeletplugin.DriverName(driverName),
		kubeletplugin.NodeName(nodeName),
		kubeletplugin.KubeClient(kubeClient),
	}
	d, err := kubeletplugin.Start(ctx, plugin, kubeletOpts...)
	if err != nil {
		return nil, fmt.Errorf("start kubelet plugin: %w", err)
	}
	plugin.draPlugin = d
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(context.Context) (bool, error) {
		status := plugin.draPlugin.RegistrationStatus()
		if status == nil {
			return false, nil
		}
		return status.PluginRegistered, nil
	})
	if err != nil {
		return nil, err
	}

	// register the NRI plugin
	nriOpts := []stub.Option{
		stub.WithPluginName(driverName),
		stub.WithPluginIdx("00"),
		// https://github.com/containerd/nri/pull/173
		// Otherwise it silently exits the program
		stub.WithOnClose(func() {
			klog.Infof("%s NRI plugin closed", driverName)
		}),
	}
	stub, err := stub.New(plugin, nriOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create plugin stub: %v", err)
	}
	plugin.nriPlugin = stub

	go func() {
		for i := 0; i < maxAttempts; i++ {
			err = plugin.nriPlugin.Run(ctx)
			if err != nil {
				klog.Infof("NRI plugin failed with error %v", err)
			}
			select {
			case <-ctx.Done():
				return
			default:
				klog.Infof("Restarting NRI plugin %d out of %d", i, maxAttempts)
			}
		}
		klog.Fatalf("NRI plugin failed for %d times to be restarted", maxAttempts)
	}()

	// register the host network interfaces
	if plugin.netdb == nil {
		plugin.netdb = inventory.New()
	}
	go func() {
		for i := 0; i < maxAttempts; i++ {
			err = plugin.netdb.Run(ctx)
			if err != nil {
				klog.Infof("Network Device DB failed with error %v", err)
			}
			select {
			case <-ctx.Done():
				return
			default:
				klog.Infof("Restarting Network Device DB %d out of %d", i, maxAttempts)
			}
		}
		klog.Fatalf("Network Device DB failed for %d times to be restarted", maxAttempts)
	}()

	// publish available resources
	go plugin.PublishResources(ctx)

	return plugin, nil
}

// reconcileStateStore removes stale entries from the state store that belong to
// pods that no longer exist on this node. This is necessary after a node restart
// because pod UIDs and network namespace paths from the previous boot are invalid.
func (np *NetworkDriver) reconcileStateStore(ctx context.Context) error {
	if np.stateStore == nil {
		return nil
	}

	// Get all pods currently running on this node from the Kubernetes API
	pods, err := np.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + np.nodeName,
	})
	if err != nil {
		return fmt.Errorf("failed to list pods on node %s: %w", np.nodeName, err)
	}

	// Build a set of active pod UIDs
	activePodUIDs := make(map[types.UID]bool)
	for _, pod := range pods.Items {
		activePodUIDs[pod.UID] = true
	}

	// Clean up stale pod configs
	storedConfigs, err := np.stateStore.ListAllPodConfigs()
	if err != nil {
		return fmt.Errorf("failed to list stored pod configs: %w", err)
	}

	var staleCount int
	for podUID := range storedConfigs {
		if !activePodUIDs[podUID] {
			if err := np.stateStore.DeletePod(podUID); err != nil {
				klog.Warningf("Failed to delete stale pod config for pod %s: %v", podUID, err)
			} else {
				staleCount++
			}
		}
	}

	// Clean up stale pod network namespace entries
	storedNetNs, err := np.stateStore.ListPodNetNs()
	if err != nil {
		return fmt.Errorf("failed to list stored pod netns: %w", err)
	}

	var staleNetNsCount int
	for podKey := range storedNetNs {
		// podKey format is typically "namespace/name" or contains the pod UID
		// We need to parse it and check if the pod still exists
		// For now, we'll check if the netns path is still valid
		// since after a restart, /proc/<pid>/ns/net paths are invalid
		netNsPath, _ := np.stateStore.GetPodNetNs(podKey)
		if _, err := os.Stat(netNsPath); os.IsNotExist(err) {
			if err := np.stateStore.DeletePodNetNs(podKey); err != nil {
				klog.Warningf("Failed to delete stale netns entry for pod %s: %v", podKey, err)
			} else {
				staleNetNsCount++
			}
		}
	}

	if staleCount > 0 || staleNetNsCount > 0 {
		klog.Infof("Reconciled state store: removed %d stale pod configs and %d stale netns entries", staleCount, staleNetNsCount)
	}

	return nil
}

func (np *NetworkDriver) Stop() {
	// Stop NRI Plugin (it's expected that it returns when fully stopped).
	np.nriPlugin.Stop()
	// Stop DRA Plugin (returns only after it has fully stopped).
	np.draPlugin.Stop()
}
