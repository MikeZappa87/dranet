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
	"sync"

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
// It supports an optional persistent backend via statestore.Store.
type PodConfigStore struct {
	mu      sync.RWMutex
	configs map[types.UID]map[string]PodConfig

	// backend is an optional persistent store. If nil, only in-memory storage is used.
	backend statestore.Store
}

// NewPodConfigStore creates and returns a new instance of PodConfigStore with in-memory storage only.
func NewPodConfigStore() *PodConfigStore {
	return &PodConfigStore{
		configs: make(map[types.UID]map[string]PodConfig),
	}
}

// NewPodConfigStoreWithBackend creates a PodConfigStore backed by the provided statestore.
// It loads any existing state from the backend on creation.
func NewPodConfigStoreWithBackend(store statestore.Store) *PodConfigStore {
	s := &PodConfigStore{
		configs: make(map[types.UID]map[string]PodConfig),
		backend: store,
	}
	// Load existing state from backend
	if store != nil {
		allConfigs, err := store.ListAllPodConfigs()
		if err != nil {
			klog.Errorf("Failed to load pod configs from backend: %v", err)
		} else {
			for podUID, deviceConfigs := range allConfigs {
				s.configs[podUID] = make(map[string]PodConfig)
				for deviceName, configBytes := range deviceConfigs {
					var config PodConfig
					if err := json.Unmarshal(configBytes, &config); err != nil {
						klog.Errorf("Failed to unmarshal config for pod %s device %s: %v", podUID, deviceName, err)
						continue
					}
					s.configs[podUID][deviceName] = config
				}
			}
			klog.Infof("Loaded %d pod configs from persistent storage", len(s.configs))
		}
	}
	return s
}

// Set stores the configuration for a specific device under a given Pod UID.
// If a configuration for the Pod UID or device name already exists, it will be overwritten.
func (s *PodConfigStore) Set(podUID types.UID, deviceName string, config PodConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.configs[podUID]; !ok {
		s.configs[podUID] = make(map[string]PodConfig)
	}
	s.configs[podUID][deviceName] = config

	// Persist to backend if available
	if s.backend != nil {
		configBytes, err := json.Marshal(config)
		if err != nil {
			klog.Errorf("Failed to marshal config for pod %s device %s: %v", podUID, deviceName, err)
			return
		}
		if err := s.backend.SetPodConfig(podUID, deviceName, configBytes); err != nil {
			klog.Errorf("Failed to persist config for pod %s device %s: %v", podUID, deviceName, err)
		}
	}
}

// Get retrieves the configuration for a specific device under a given Pod UID.
// It returns the Config and true if found, otherwise an empty Config and false.
func (s *PodConfigStore) Get(podUID types.UID, deviceName string) (PodConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if podConfigs, ok := s.configs[podUID]; ok {
		config, found := podConfigs[deviceName]
		return config, found
	}
	return PodConfig{}, false
}

// DeletePod removes all configurations associated with a given Pod UID.
func (s *PodConfigStore) DeletePod(podUID types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.configs, podUID)

	// Remove from backend if available
	if s.backend != nil {
		if err := s.backend.DeletePod(podUID); err != nil {
			klog.Errorf("Failed to delete pod %s from backend: %v", podUID, err)
		}
	}
}

// GetPodConfigs retrieves all device configurations for a given Pod UID.
// It is indexed by the Pod's UID, and for each Pod, it maps network device names (as allocated)
// to their specific Config.
func (s *PodConfigStore) GetPodConfigs(podUID types.UID) (map[string]PodConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	podConfigs, found := s.configs[podUID]
	if !found {
		return nil, false
	}
	// Return a copy to prevent external modification of the internal map
	configsCopy := make(map[string]PodConfig, len(podConfigs))
	for k, v := range podConfigs {
		configsCopy[k] = v
	}
	return configsCopy, true
}

// DeleteClaim removes all configurations associated with a given claim.
func (s *PodConfigStore) DeleteClaim(claim types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	podsToDelete := []types.UID{}
	for uid, podConfigsMap := range s.configs {
		for _, config := range podConfigsMap {
			if config.Claim == claim {
				podsToDelete = append(podsToDelete, uid)
				break // Found a match for this pod, no need to check other devices for the same pod
			}
		}
	}

	for _, uid := range podsToDelete {
		delete(s.configs, uid)
	}

	// Remove from backend if available
	if s.backend != nil {
		if err := s.backend.DeletePodConfigsByClaim(claim.Namespace, claim.Name); err != nil {
			klog.Errorf("Failed to delete configs for claim %s from backend: %v", claim, err)
		}
	}
}
