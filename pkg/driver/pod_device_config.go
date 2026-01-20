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
	"encoding/json"

	"github.com/google/dranet/pkg/apis"
	"github.com/google/dranet/pkg/statestore"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// PodConfig holds the set of configurations to be applied for a single
// network device allocated to a Pod. This includes network interface settings,
// routes for the Pod's network namespace, and RDMA configurations.
type PodConfig struct {
	Claim types.NamespacedName

	// NetworkInterfaceConfigInHost is the config of the network interface as
	// seen in the host's network namespace BEFORE it was moved to the pod's
	// network namespace.
	NetworkInterfaceConfigInHost apis.NetworkConfig

	// NetworkInterfaceConfigInPod contains all network-related configurations
	// (interface, routes, ethtool, sysctl) to be applied for this device in the
	// Pod's namespace.
	NetworkInterfaceConfigInPod apis.NetworkConfig

	// RDMADevice holds RDMA-specific configurations if the network device
	// has associated RDMA capabilities.
	RDMADevice RDMAConfig
}

// RDMAConfig contains parameters for setting up an RDMA device associated
// with a network interface.
type RDMAConfig struct {
	// LinkDev is the name of the RDMA link device (e.g., "mlx5_0") that
	// corresponds to the allocated network device.
	LinkDev string

	// DevChars is a list of user-space RDMA character
	// devices (e.g., "/dev/infiniband/uverbs0", "/dev/infiniband/rdma_cm")
	// that should be made available to the Pod.
	DevChars []LinuxDevice
}

type LinuxDevice struct {
	Path     string
	Type     string
	Major    int64
	Minor    int64
	FileMode uint32
	UID      uint32
	GID      uint32
}

// PodConfigStore provides a thread-safe, centralized store for all network device configurations
// across multiple Pods. It is indexed by the Pod's UID, and for each Pod, it maps
// network device names (as allocated) to their specific Config.
// It uses a statestore.Store backend for storage (memory or persistent).
type PodConfigStore struct {
	backend statestore.Store
}

// NewPodConfigStore creates and returns a new instance of PodConfigStore with in-memory storage.
func NewPodConfigStore() *PodConfigStore {
	backend := statestore.New(statestore.Config{Type: statestore.TypeMemory})
	return &PodConfigStore{
		backend: backend,
	}
}

// NewPodConfigStoreWithBackend creates a PodConfigStore backed by the provided statestore.
// If store is nil, an in-memory backend is used.
func NewPodConfigStoreWithBackend(store statestore.Store) *PodConfigStore {
	if store == nil {
		return NewPodConfigStore()
	}
	return &PodConfigStore{
		backend: store,
	}
}

// Set stores the configuration for a specific device under a given Pod UID.
// If a configuration for the Pod UID or device name already exists, it will be overwritten.
// For DRA store backends, this also registers the claim device mapping if not already done.
func (s *PodConfigStore) Set(podUID types.UID, deviceName string, config PodConfig) {
	// If the backend supports DRA extension, register the claim device mapping
	// This is required for the DRA store to know where to persist the config
	if ext, ok := s.backend.(statestore.DRAStoreExtension); ok {
		ext.RegisterClaimDevice(podUID, deviceName, config.Claim.Namespace, config.Claim.Name, "")
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		klog.Errorf("Failed to marshal config for pod %s device %s: %v", podUID, deviceName, err)
		return
	}
	if err := s.backend.SetPodConfig(podUID, deviceName, configBytes); err != nil {
		klog.Errorf("Failed to store config for pod %s device %s: %v", podUID, deviceName, err)
	}
}

// Get retrieves the configuration for a specific device under a given Pod UID.
// It returns the Config and true if found, otherwise an empty Config and false.
func (s *PodConfigStore) Get(podUID types.UID, deviceName string) (PodConfig, bool) {
	configBytes, found := s.backend.GetPodConfig(podUID, deviceName)
	if !found {
		return PodConfig{}, false
	}
	var config PodConfig
	if err := json.Unmarshal(configBytes, &config); err != nil {
		klog.Errorf("Failed to unmarshal config for pod %s device %s: %v", podUID, deviceName, err)
		return PodConfig{}, false
	}
	return config, true
}

// DeletePod removes all configurations associated with a given Pod UID.
func (s *PodConfigStore) DeletePod(podUID types.UID) {
	if err := s.backend.DeletePod(podUID); err != nil {
		klog.Errorf("Failed to delete pod %s: %v", podUID, err)
	}
}

// GetPodConfigs retrieves all device configurations for a given Pod UID.
// It is indexed by the Pod's UID, and for each Pod, it maps network device names (as allocated)
// to their specific Config.
func (s *PodConfigStore) GetPodConfigs(podUID types.UID) (map[string]PodConfig, bool) {
	podConfigsBytes, found := s.backend.GetPodConfigs(podUID)
	if !found {
		return nil, false
	}
	result := make(map[string]PodConfig, len(podConfigsBytes))
	for deviceName, configBytes := range podConfigsBytes {
		var config PodConfig
		if err := json.Unmarshal(configBytes, &config); err != nil {
			klog.Errorf("Failed to unmarshal config for pod %s device %s: %v", podUID, deviceName, err)
			continue
		}
		result[deviceName] = config
	}
	return result, true
}

// DeleteClaim removes all configurations associated with a given claim.
func (s *PodConfigStore) DeleteClaim(claim types.NamespacedName) {
	if err := s.backend.DeletePodConfigsByClaim(claim.Namespace, claim.Name); err != nil {
		klog.Errorf("Failed to delete configs for claim %s: %v", claim, err)
	}
}
