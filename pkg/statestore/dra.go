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

package statestore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	resourceapply "k8s.io/client-go/applyconfigurations/resource/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// DRADataKey is the key used in AllocatedDeviceStatus.Data to store pod configuration
const DRADataKey = "dranet.io/pod-config"

// draStore implements Store using DRA's AllocatedDeviceStatus.Data field
// to persist pod configuration in the ResourceClaim status instead of on disk.
//
// This approach leverages the native DRA mechanism where:
// - Configuration is stored in AllocatedDeviceStatus.Data (up to 10Ki per device)
// - State is automatically tied to the ResourceClaim lifecycle
// - No disk dependencies for state persistence
// - State survives driver restarts as long as ResourceClaims exist
//
// Limitations:
// - Requires network connectivity to the API server
// - Slightly higher latency compared to local disk
// - 10Ki limit per device for the Data field
type draStore struct {
	kubeClient   kubernetes.Interface
	driverName   string
	nodeName     string
	fieldManager string

	mu sync.RWMutex

	// podNetNs maps pod keys to network namespace paths (kept in memory)
	// This is ephemeral data that doesn't need to persist across restarts
	podNetNs map[string]string

	// claimCache caches claim info for pods: podUID -> deviceName -> claimRef
	// This allows us to look up which claim a device belongs to
	claimCache map[types.UID]map[string]claimDeviceRef

	// podConfigs caches the actual config data in memory: podUID -> deviceName -> configBytes
	// This is the primary source for reads (fast), while ResourceClaim is used for persistence
	podConfigs map[types.UID]map[string][]byte
}

// claimDeviceRef holds the reference to locate a device's config in a ResourceClaim
type claimDeviceRef struct {
	Namespace  string
	Name       string
	Pool       string
	DeviceName string
}

// DRAStoreConfig holds configuration for creating a DRA-based state store.
type DRAStoreConfig struct {
	KubeClient   kubernetes.Interface
	DriverName   string
	NodeName     string
	FieldManager string
}

// newDRAStore creates a new DRA-based state store.
func newDRAStore(cfg DRAStoreConfig) *draStore {
	fieldManager := cfg.FieldManager
	if fieldManager == "" {
		fieldManager = cfg.DriverName
	}
	return &draStore{
		kubeClient:   cfg.KubeClient,
		driverName:   cfg.DriverName,
		nodeName:     cfg.NodeName,
		fieldManager: fieldManager,
		podNetNs:     make(map[string]string),
		claimCache:   make(map[types.UID]map[string]claimDeviceRef),
		podConfigs:   make(map[types.UID]map[string][]byte),
	}
}

func (s *draStore) Open() error {
	if s.kubeClient == nil {
		return fmt.Errorf("kubernetes client is required for DRA store")
	}

	// Rebuild the cache from existing ResourceClaims on this node.
	// This is critical for surviving driver restarts - without this,
	// NRI hooks would fail because GetPodConfigs() wouldn't find any cache entries.
	if err := s.rebuildCacheFromClaims(); err != nil {
		// Log but don't fail - the cache will be rebuilt as PrepareResourceClaims is called
		klog.Warningf("Failed to rebuild cache from ResourceClaims on startup: %v", err)
	}

	klog.Info("Opened DRA-based state store (using ResourceClaim status)")
	return nil
}

// rebuildCacheFromClaims lists all ResourceClaims allocated to this node
// and rebuilds the claimCache. This ensures the driver can serve NRI hooks
// immediately after restart, before PrepareResourceClaims is called.
func (s *draStore) rebuildCacheFromClaims() error {
	ctx := context.Background()

	// List all ResourceClaims in all namespaces
	// We filter by driver name in the processing loop
	claimList, err := s.kubeClient.ResourceV1().ResourceClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list ResourceClaims: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	claimsLoaded := 0
	for i := range claimList.Items {
		claim := &claimList.Items[i]

		// Skip claims that aren't allocated or don't have devices from our driver
		if claim.Status.Allocation == nil {
			continue
		}

		// Check if any device in this claim is managed by our driver and on our node
		hasOurDevices := false
		for _, result := range claim.Status.Allocation.Devices.Results {
			if result.Driver == s.driverName && result.Pool == s.nodeName {
				hasOurDevices = true
				break
			}
		}
		if !hasOurDevices {
			continue
		}

		// Load the claim into our cache
		s.loadFromResourceClaimLocked(claim)
		claimsLoaded++
	}

	klog.Infof("Rebuilt cache from %d ResourceClaims on startup", claimsLoaded)
	return nil
}

func (s *draStore) Close() error {
	klog.Info("Closing DRA-based state store")
	return nil
}

// Pod network namespace operations - these remain in-memory as they're ephemeral

func (s *draStore) SetPodNetNs(podKey string, netNsPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.podNetNs[podKey] = netNsPath
	return nil
}

func (s *draStore) GetPodNetNs(podKey string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, ok := s.podNetNs[podKey]
	return ns, ok
}

func (s *draStore) DeletePodNetNs(podKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.podNetNs, podKey)
	return nil
}

func (s *draStore) ListPodNetNs() (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]string, len(s.podNetNs))
	for k, v := range s.podNetNs {
		result[k] = v
	}
	return result, nil
}

// Pod config operations - these store/retrieve data from ResourceClaim status

// SetPodConfig stores the configuration for a device.
// The config is stored in memory for fast access, and asynchronously persisted to the
// ResourceClaim's AllocatedDeviceStatus.Data field for recovery after restarts.
func (s *draStore) SetPodConfig(podUID types.UID, deviceName string, config []byte) error {
	s.mu.Lock()
	// Always store in memory cache for fast reads
	if s.podConfigs[podUID] == nil {
		s.podConfigs[podUID] = make(map[string][]byte)
	}
	s.podConfigs[podUID][deviceName] = config
	klog.Infof("SetPodConfig: stored config in memory for pod %s device %s", podUID, deviceName)

	ref, ok := s.getClaimRefLocked(podUID, deviceName)
	s.mu.Unlock()

	if !ok {
		// If we don't have a cached reference, we can't persist to DRA
		// The in-memory config will still work for this session
		klog.V(4).Infof("No claim reference cached for pod %s device %s, config will not persist to DRA", podUID, deviceName)
		return nil
	}

	// Persist to ResourceClaim asynchronously (best effort)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.updateDeviceData(ctx, ref, podUID, config); err != nil {
			klog.Warningf("Failed to persist config to ResourceClaim for pod %s device %s: %v", podUID, deviceName, err)
		} else {
			klog.V(4).Infof("Persisted config to ResourceClaim for pod %s device %s", podUID, deviceName)
		}
	}()

	return nil
}

// RegisterClaimDevice registers the mapping from (podUID, deviceName) to a ResourceClaim.
// This must be called during PrepareResourceClaim so that SetPodConfig knows where to store data.
// If pool is empty, the store's nodeName is used as the default pool.
func (s *draStore) RegisterClaimDevice(podUID types.UID, deviceName string, claimNamespace, claimName, pool string) {
	klog.Infof("RegisterClaimDevice: podUID=%s device=%s claim=%s/%s", podUID, deviceName, claimNamespace, claimName)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Use nodeName as default pool if not specified
	if pool == "" {
		pool = s.nodeName
	}

	if s.claimCache[podUID] == nil {
		s.claimCache[podUID] = make(map[string]claimDeviceRef)
	}
	s.claimCache[podUID][deviceName] = claimDeviceRef{
		Namespace:  claimNamespace,
		Name:       claimName,
		Pool:       pool,
		DeviceName: deviceName,
	}
}

func (s *draStore) getClaimRefLocked(podUID types.UID, deviceName string) (claimDeviceRef, bool) {
	devices, ok := s.claimCache[podUID]
	if !ok {
		return claimDeviceRef{}, false
	}
	ref, ok := devices[deviceName]
	return ref, ok
}

// updateDeviceData updates the Data field of a device's AllocatedDeviceStatus in the ResourceClaim.
func (s *draStore) updateDeviceData(ctx context.Context, ref claimDeviceRef, podUID types.UID, config []byte) error {
	// Wrap the config with the pod UID as a key to support multiple pods sharing a device
	dataWrapper := map[string]json.RawMessage{
		string(podUID): config,
	}
	dataBytes, err := json.Marshal(dataWrapper)
	if err != nil {
		return fmt.Errorf("failed to marshal config data: %w", err)
	}

	// Build the apply configuration for the device status
	deviceStatus := resourceapply.AllocatedDeviceStatus().
		WithDriver(s.driverName).
		WithPool(ref.Pool).
		WithDevice(ref.DeviceName).
		WithData(runtime.RawExtension{Raw: dataBytes})

	claimStatus := resourceapply.ResourceClaimStatus().
		WithDevices(deviceStatus)

	claimApply := resourceapply.ResourceClaim(ref.Name, ref.Namespace).
		WithStatus(claimStatus)

	_, err = s.kubeClient.ResourceV1().ResourceClaims(ref.Namespace).ApplyStatus(
		ctx,
		claimApply,
		metav1.ApplyOptions{FieldManager: s.fieldManager, Force: true},
	)
	if err != nil {
		return fmt.Errorf("failed to update ResourceClaim %s/%s status: %w", ref.Namespace, ref.Name, err)
	}

	klog.V(4).Infof("Stored config for pod %s device %s in ResourceClaim %s/%s", podUID, ref.DeviceName, ref.Namespace, ref.Name)
	return nil
}

// GetPodConfig retrieves the configuration for a device.
// First checks the in-memory cache, falls back to ResourceClaim on cache miss.
func (s *draStore) GetPodConfig(podUID types.UID, deviceName string) ([]byte, bool) {
	s.mu.RLock()
	// First check in-memory cache
	if devices, ok := s.podConfigs[podUID]; ok {
		if config, ok := devices[deviceName]; ok {
			s.mu.RUnlock()
			return config, true
		}
	}

	ref, ok := s.getClaimRefLocked(podUID, deviceName)
	s.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// Fallback to ResourceClaim (for recovery after restart)
	ctx := context.Background()
	claim, err := s.kubeClient.ResourceV1().ResourceClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		klog.V(4).Infof("Failed to get ResourceClaim %s/%s: %v", ref.Namespace, ref.Name, err)
		return nil, false
	}

	return s.extractConfigFromClaim(claim, ref.DeviceName, podUID)
}

// extractConfigFromClaim extracts the pod configuration from a ResourceClaim's device status.
func (s *draStore) extractConfigFromClaim(claim *resourceapi.ResourceClaim, deviceName string, podUID types.UID) ([]byte, bool) {
	klog.Infof("extractConfigFromClaim: looking for device %s (driver=%s) in claim %s/%s with %d status devices",
		deviceName, s.driverName, claim.Namespace, claim.Name, len(claim.Status.Devices))
	for i, device := range claim.Status.Devices {
		klog.Infof("extractConfigFromClaim: status device[%d]: driver=%s device=%s pool=%s hasData=%v",
			i, device.Driver, device.Device, device.Pool, device.Data != nil && len(device.Data.Raw) > 0)
		if device.Driver != s.driverName || device.Device != deviceName {
			continue
		}
		if device.Data == nil || len(device.Data.Raw) == 0 {
			klog.Infof("extractConfigFromClaim: found device %s but Data is nil or empty", deviceName)
			return nil, false
		}

		// Parse the data wrapper to get the config for this specific pod
		var dataWrapper map[string]json.RawMessage
		if err := json.Unmarshal(device.Data.Raw, &dataWrapper); err != nil {
			klog.V(4).Infof("Failed to unmarshal device data for %s: %v", deviceName, err)
			return nil, false
		}

		config, ok := dataWrapper[string(podUID)]
		if !ok {
			return nil, false
		}
		return config, true
	}
	return nil, false
}

// GetPodConfigs retrieves all device configurations for a given Pod UID.
// First checks the in-memory cache, falls back to ResourceClaim on cache miss.
func (s *draStore) GetPodConfigs(podUID types.UID) (map[string][]byte, bool) {
	s.mu.RLock()
	// First check in-memory cache
	if configs, ok := s.podConfigs[podUID]; ok && len(configs) > 0 {
		// Make a copy to avoid holding lock
		result := make(map[string][]byte, len(configs))
		for k, v := range configs {
			result[k] = v
		}
		s.mu.RUnlock()
		klog.Infof("GetPodConfigs: podUID %s found in memory cache with %d devices", podUID, len(result))
		return result, true
	}

	// Check if we have claim refs to try ResourceClaim fallback
	devices, ok := s.claimCache[podUID]
	if !ok {
		klog.Infof("GetPodConfigs: podUID %s not found in claimCache or memory (cache has %d entries)", podUID, len(s.claimCache))
		s.mu.RUnlock()
		return nil, false
	}
	klog.Infof("GetPodConfigs: podUID %s found in claimCache with %d devices, falling back to ResourceClaim", podUID, len(devices))
	// Make a copy of the device refs to avoid holding the lock during API calls
	refs := make(map[string]claimDeviceRef, len(devices))
	for k, v := range devices {
		refs[k] = v
	}
	s.mu.RUnlock()

	// Fallback to ResourceClaim (for recovery after restart)
	result := make(map[string][]byte)
	ctx := context.Background()

	// Group devices by claim to minimize API calls
	claimDevices := make(map[string][]string) // claimKey -> deviceNames
	claimRefs := make(map[string]claimDeviceRef)
	for deviceName, ref := range refs {
		claimKey := ref.Namespace + "/" + ref.Name
		claimDevices[claimKey] = append(claimDevices[claimKey], deviceName)
		claimRefs[claimKey] = ref
	}

	for claimKey, deviceNames := range claimDevices {
		ref := claimRefs[claimKey]
		claim, err := s.kubeClient.ResourceV1().ResourceClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			klog.V(4).Infof("Failed to get ResourceClaim %s/%s: %v", ref.Namespace, ref.Name, err)
			continue
		}

		for _, deviceName := range deviceNames {
			if config, ok := s.extractConfigFromClaim(claim, deviceName, podUID); ok {
				result[deviceName] = config
			} else {
				klog.Infof("GetPodConfigs: failed to extract config for device %s from claim %s/%s", deviceName, ref.Namespace, ref.Name)
			}
		}
	}

	if len(result) == 0 {
		klog.Infof("GetPodConfigs: claimCache had entries but no configs extracted from ResourceClaims")
		return nil, false
	}
	klog.Infof("GetPodConfigs: returning %d device configs", len(result))
	return result, true
}

// DeletePod removes all configurations associated with a given Pod UID.
// For DRA store, this clears both the in-memory config cache and the claim cache.
// The actual data in ResourceClaims will be cleaned up when the claim is deallocated.
func (s *draStore) DeletePod(podUID types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.claimCache, podUID)
	delete(s.podConfigs, podUID)
	klog.Infof("DeletePod: removed pod %s from caches", podUID)
	return nil
}

// DeletePodConfigsByClaim is a no-op for the DRA store.
// Unlike memory/bbolt stores where data is keyed by podUID, the DRA store uses a
// cache that maps podUID → claimRef. If we clear this cache when the claim is
// unprepared, StopPodSandbox won't be able to find the configs to move devices back.
//
// The cache will be cleaned up when:
// - DeletePod is called (when the pod is removed)
// - The ResourceClaim is deleted from the cluster (GetPodConfig will fail gracefully)
func (s *draStore) DeletePodConfigsByClaim(claimNamespace, claimName string) error {
	// Intentionally do nothing - keep the cache intact so StopPodSandbox can still
	// find configs. The DRA UnprepareResourceClaims hook is often called BEFORE
	// the NRI StopPodSandbox hook.
	klog.V(4).Infof("DeletePodConfigsByClaim(%s/%s): no-op for DRA store (cache preserved for StopPodSandbox)", claimNamespace, claimName)
	return nil
}

// ListAllPodConfigs returns all stored configurations.
// Note: This requires fetching from multiple ResourceClaims and may be expensive.
func (s *draStore) ListAllPodConfigs() (map[types.UID]map[string][]byte, error) {
	s.mu.RLock()
	// Make a copy of the cache to avoid holding the lock during API calls
	cacheCopy := make(map[types.UID]map[string]claimDeviceRef)
	for podUID, devices := range s.claimCache {
		cacheCopy[podUID] = make(map[string]claimDeviceRef)
		for k, v := range devices {
			cacheCopy[podUID][k] = v
		}
	}
	s.mu.RUnlock()

	result := make(map[types.UID]map[string][]byte)
	for podUID := range cacheCopy {
		configs, ok := s.GetPodConfigs(podUID)
		if ok && len(configs) > 0 {
			result[podUID] = configs
		}
	}
	return result, nil
}

// LoadFromClaim populates the cache from a ResourceClaim during PrepareResourceClaim.
// This should be called to restore state after a driver restart.
// The claim parameter should be a *resourceapi.ResourceClaim.
func (s *draStore) LoadFromClaim(claim interface{}) {
	rc, ok := claim.(*resourceapi.ResourceClaim)
	if !ok {
		klog.Warningf("LoadFromClaim: expected *ResourceClaim, got %T", claim)
		return
	}
	s.loadFromResourceClaim(rc)
}

func (s *draStore) loadFromResourceClaim(claim *resourceapi.ResourceClaim) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadFromResourceClaimLocked(claim)
}

// loadFromResourceClaimLocked populates the cache from a ResourceClaim.
// Caller must hold s.mu lock.
func (s *draStore) loadFromResourceClaimLocked(claim *resourceapi.ResourceClaim) {
	if claim.Status.Allocation == nil {
		return
	}

	for _, reserved := range claim.Status.ReservedFor {
		if reserved.Resource != "pods" || reserved.APIGroup != "" {
			continue
		}
		podUID := reserved.UID

		// Use Allocation.Devices.Results (the scheduler's allocation) rather than
		// Status.Devices (the driver's runtime status output). Status.Devices is only
		// populated AFTER RunPodSandbox completes, but we need the cache to be populated
		// BEFORE that so PrepareResourceClaims and NRI hooks can find configs.
		for _, result := range claim.Status.Allocation.Devices.Results {
			if result.Driver != s.driverName {
				continue
			}

			if s.claimCache[podUID] == nil {
				s.claimCache[podUID] = make(map[string]claimDeviceRef)
			}
			s.claimCache[podUID][result.Device] = claimDeviceRef{
				Namespace:  claim.Namespace,
				Name:       claim.Name,
				Pool:       result.Pool,
				DeviceName: result.Device,
			}
			klog.V(4).Infof("loadFromResourceClaimLocked: cached pod %s device %s -> claim %s/%s pool %s",
				podUID, result.Device, claim.Namespace, claim.Name, result.Pool)
		}

		// Also try to load config data from Status.Devices if available
		// This is needed for recovery after restart when the pod was already running
		for _, device := range claim.Status.Devices {
			if device.Driver != s.driverName {
				continue
			}
			if device.Data == nil || len(device.Data.Raw) == 0 {
				continue
			}

			// Parse the data wrapper to get the config for this pod
			var dataWrapper map[string]json.RawMessage
			if err := json.Unmarshal(device.Data.Raw, &dataWrapper); err != nil {
				klog.V(4).Infof("loadFromResourceClaimLocked: failed to unmarshal device data for %s: %v", device.Device, err)
				continue
			}

			config, ok := dataWrapper[string(podUID)]
			if !ok {
				continue
			}

			if s.podConfigs[podUID] == nil {
				s.podConfigs[podUID] = make(map[string][]byte)
			}
			s.podConfigs[podUID][device.Device] = config
			klog.Infof("loadFromResourceClaimLocked: restored config for pod %s device %s from claim %s/%s",
				podUID, device.Device, claim.Namespace, claim.Name)
		}
	}
}

// Ensure draStore implements the Store interface
var _ Store = (*draStore)(nil)

// Ensure draStore implements the DRAStoreExtension interface
var _ DRAStoreExtension = (*draStore)(nil)

// Compile-time check for unused import
var _ = metav1apply.Condition
