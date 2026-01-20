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
	"testing"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDRAStore(t *testing.T) {
	const (
		driverName     = "dranet.google.com"
		nodeName       = "test-node"
		claimNamespace = "default"
		claimName      = "test-claim"
		podUID         = types.UID("test-pod-uid-12345")
		deviceName     = "eth0"
	)

	// Create a fake claim
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: claimNamespace,
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{
					Resource: "pods",
					Name:     "test-pod",
					UID:      podUID,
				},
			},
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver:  driverName,
							Pool:    nodeName,
							Device:  deviceName,
							Request: "net",
						},
					},
				},
			},
			Devices: []resourceapi.AllocatedDeviceStatus{
				{
					Driver: driverName,
					Pool:   nodeName,
					Device: deviceName,
				},
			},
		},
	}

	// Create fake client with the claim
	fakeClient := fake.NewClientset(claim)

	// Create the DRA store
	store := newDRAStore(DRAStoreConfig{
		KubeClient:   fakeClient,
		DriverName:   driverName,
		NodeName:     nodeName,
		FieldManager: driverName,
	})

	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open DRA store: %v", err)
	}
	defer store.Close()

	t.Run("PodNetNs", func(t *testing.T) {
		// PodNetNs operations work the same as memory store
		testPodNetNsOperations(t, store)
	})

	t.Run("RegisterAndSetPodConfig", func(t *testing.T) {
		// First, register the claim device mapping
		store.RegisterClaimDevice(podUID, deviceName, claimNamespace, claimName, nodeName)

		// Create a config
		config := testConfig{
			InterfaceName: "eth0",
		}
		config.Claim.Namespace = claimNamespace
		config.Claim.Name = claimName

		configBytes, err := json.Marshal(config)
		if err != nil {
			t.Fatalf("Failed to marshal config: %v", err)
		}

		// Set the config
		if err := store.SetPodConfig(podUID, deviceName, configBytes); err != nil {
			t.Fatalf("SetPodConfig failed: %v", err)
		}

		// Verify we can get the config back from in-memory cache immediately
		gotConfig, ok := store.GetPodConfig(podUID, deviceName)
		if !ok {
			t.Fatal("GetPodConfig returned not found")
		}

		var gotTestConfig testConfig
		if err := json.Unmarshal(gotConfig, &gotTestConfig); err != nil {
			t.Fatalf("Failed to unmarshal retrieved config: %v", err)
		}

		if gotTestConfig.InterfaceName != config.InterfaceName {
			t.Errorf("Retrieved InterfaceName = %q, want %q", gotTestConfig.InterfaceName, config.InterfaceName)
		}

		// Give async write time to complete, then verify ResourceClaim was updated
		time.Sleep(100 * time.Millisecond)

		updatedClaim, err := fakeClient.ResourceV1().ResourceClaims(claimNamespace).Get(context.Background(), claimName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get updated claim: %v", err)
		}

		// Find our device in the status
		var foundDevice *resourceapi.AllocatedDeviceStatus
		for i := range updatedClaim.Status.Devices {
			if updatedClaim.Status.Devices[i].Device == deviceName &&
				updatedClaim.Status.Devices[i].Driver == driverName {
				foundDevice = &updatedClaim.Status.Devices[i]
				break
			}
		}

		if foundDevice == nil {
			t.Fatal("Device not found in claim status after SetPodConfig")
		}

		if foundDevice.Data == nil || len(foundDevice.Data.Raw) == 0 {
			t.Fatal("Device data is empty after SetPodConfig")
		}
	})

	t.Run("GetPodConfigs", func(t *testing.T) {
		configs, ok := store.GetPodConfigs(podUID)
		if !ok {
			t.Fatal("GetPodConfigs returned not found")
		}

		if len(configs) != 1 {
			t.Errorf("GetPodConfigs returned %d configs, want 1", len(configs))
		}

		if _, exists := configs[deviceName]; !exists {
			t.Errorf("GetPodConfigs missing device %s", deviceName)
		}
	})

	t.Run("LoadFromClaim", func(t *testing.T) {
		// Create a claim with pre-existing Data (simulating recovery after restart)
		claimWithData := &resourceapi.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "claim-with-data",
				Namespace: claimNamespace,
			},
			Status: resourceapi.ResourceClaimStatus{
				Allocation: &resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							{
								Request: "test-request",
								Driver:  driverName,
								Pool:    nodeName,
								Device:  deviceName,
							},
						},
					},
				},
				ReservedFor: []resourceapi.ResourceClaimConsumerReference{
					{
						Resource: "pods",
						Name:     "test-pod",
						UID:      podUID,
					},
				},
				Devices: []resourceapi.AllocatedDeviceStatus{
					{
						Driver: driverName,
						Pool:   nodeName,
						Device: deviceName,
						Data: &runtime.RawExtension{
							Raw: []byte(`{"` + string(podUID) + `":{"interfaceName":"eth0","mtu":1500}}`),
						},
					},
				},
			},
		}

		// Create a new fake client with this claim
		fakeClient2 := fake.NewClientset(claimWithData)

		// Create a new store instance to test LoadFromClaim
		store2 := newDRAStore(DRAStoreConfig{
			KubeClient:   fakeClient2,
			DriverName:   driverName,
			NodeName:     nodeName,
			FieldManager: driverName,
		})
		if err := store2.Open(); err != nil {
			t.Fatalf("Failed to open second DRA store: %v", err)
		}
		defer store2.Close()

		// Load from the claim (this should populate both claimCache and podConfigs)
		store2.LoadFromClaim(claimWithData)

		// Now we should be able to get the config
		configs, ok := store2.GetPodConfigs(podUID)
		if !ok {
			t.Fatal("GetPodConfigs returned not found after LoadFromClaim")
		}

		if len(configs) != 1 {
			t.Errorf("GetPodConfigs returned %d configs after LoadFromClaim, want 1", len(configs))
		}
	})

	t.Run("DeletePod", func(t *testing.T) {
		if err := store.DeletePod(podUID); err != nil {
			t.Fatalf("DeletePod failed: %v", err)
		}

		// The cache should be cleared
		_, ok := store.GetPodConfigs(podUID)
		if ok {
			t.Error("GetPodConfigs should return false after DeletePod")
		}
	})
}

func TestDRAStoreWithoutKubeClient(t *testing.T) {
	store := newDRAStore(DRAStoreConfig{
		KubeClient: nil,
		DriverName: "test",
	})

	err := store.Open()
	if err == nil {
		t.Error("Open should fail without a kube client")
	}
}

func TestDRAStoreInterface(t *testing.T) {
	// Verify DRA store implements both interfaces
	store := newDRAStore(DRAStoreConfig{
		KubeClient: fake.NewClientset(),
		DriverName: "test",
	})

	// Check Store interface
	var _ Store = store

	// Check DRAStoreExtension interface
	var _ DRAStoreExtension = store
}

func TestNewDRAStoreViaConfig(t *testing.T) {
	fakeClient := fake.NewClientset()

	store := New(Config{
		Type:       TypeDRA,
		KubeClient: fakeClient,
		DriverName: "dranet.google.com",
		NodeName:   "test-node",
	})

	// Verify it's a DRA store by checking it implements DRAStoreExtension
	_, ok := store.(DRAStoreExtension)
	if !ok {
		t.Error("Store created with TypeDRA should implement DRAStoreExtension")
	}
}

// TestDRAStoreRestartRecovery tests that a new DRA store instance can recover
// state from existing ResourceClaims - simulating a driver pod restart.
func TestDRAStoreRestartRecovery(t *testing.T) {
	const (
		driverName     = "dranet.google.com"
		nodeName       = "test-node"
		claimNamespace = "default"
		claimName      = "restart-test-claim"
		podUID         = types.UID("restart-pod-uid-12345")
		deviceName     = "eth0"
	)

	// Create a claim that already has allocation results and device status
	// (simulating a claim that was previously processed by the driver)
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: claimNamespace,
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{
					Resource: "pods",
					Name:     "restart-test-pod",
					UID:      podUID,
				},
			},
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver:  driverName,
							Pool:    nodeName, // This claim is allocated on our node
							Device:  deviceName,
							Request: "net",
						},
					},
				},
			},
			Devices: []resourceapi.AllocatedDeviceStatus{
				{
					Driver: driverName,
					Pool:   nodeName,
					Device: deviceName,
					// Data field contains config stored by previous driver instance
					Data: &runtime.RawExtension{
						Raw: []byte(`{"` + string(podUID) + `":{"InterfaceName":"eth0","Claim":{"Namespace":"default","Name":"restart-test-claim"}}}`),
					},
				},
			},
		},
	}

	// Create fake client with the pre-existing claim
	fakeClient := fake.NewClientset(claim)

	// Create a NEW store instance (simulating driver restart)
	store := newDRAStore(DRAStoreConfig{
		KubeClient:   fakeClient,
		DriverName:   driverName,
		NodeName:     nodeName,
		FieldManager: driverName,
	})

	// Open() should rebuild the cache from existing claims
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open DRA store: %v", err)
	}
	defer store.Close()

	// The key test: After Open(), we should be able to get configs
	// WITHOUT calling RegisterClaimDevice or LoadFromClaim explicitly.
	// This simulates NRI hooks being called immediately after driver restart.
	configs, ok := store.GetPodConfigs(podUID)
	if !ok {
		t.Fatal("GetPodConfigs returned not found - cache was not rebuilt on Open()")
	}

	if len(configs) != 1 {
		t.Errorf("GetPodConfigs returned %d configs, want 1", len(configs))
	}

	configBytes, exists := configs[deviceName]
	if !exists {
		t.Fatalf("GetPodConfigs missing device %s", deviceName)
	}

	// Verify the config content
	var config testConfig
	if err := json.Unmarshal(configBytes, &config); err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}

	if config.InterfaceName != "eth0" {
		t.Errorf("InterfaceName = %q, want %q", config.InterfaceName, "eth0")
	}
}

// TestDRAStoreIgnoresOtherNodeClaims verifies that the cache rebuild
// only loads claims allocated to this node.
func TestDRAStoreIgnoresOtherNodeClaims(t *testing.T) {
	const (
		driverName     = "dranet.google.com"
		nodeName       = "my-node"
		claimNamespace = "default"
	)

	// Create claims on different nodes
	claimOnMyNode := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim-on-my-node",
			Namespace: claimNamespace,
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{Resource: "pods", Name: "pod1", UID: "pod1-uid"},
			},
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{Driver: driverName, Pool: nodeName, Device: "eth0"},
					},
				},
			},
			Devices: []resourceapi.AllocatedDeviceStatus{
				{
					Driver: driverName,
					Pool:   nodeName,
					Device: "eth0",
					Data: &runtime.RawExtension{
						Raw: []byte(`{"pod1-uid":{"InterfaceName":"eth0"}}`),
					},
				},
			},
		},
	}

	claimOnOtherNode := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim-on-other-node",
			Namespace: claimNamespace,
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{Resource: "pods", Name: "pod2", UID: "pod2-uid"},
			},
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{Driver: driverName, Pool: "other-node", Device: "eth0"},
					},
				},
			},
			Devices: []resourceapi.AllocatedDeviceStatus{
				{
					Driver: driverName,
					Pool:   "other-node",
					Device: "eth0",
					Data: &runtime.RawExtension{
						Raw: []byte(`{"pod2-uid":{"InterfaceName":"eth0"}}`),
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientset(claimOnMyNode, claimOnOtherNode)

	store := newDRAStore(DRAStoreConfig{
		KubeClient: fakeClient,
		DriverName: driverName,
		NodeName:   nodeName,
	})

	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	// Should find config for pod on my node
	_, ok := store.GetPodConfigs("pod1-uid")
	if !ok {
		t.Error("Should find config for pod on my node")
	}

	// Should NOT find config for pod on other node
	_, ok = store.GetPodConfigs("pod2-uid")
	if ok {
		t.Error("Should NOT find config for pod on other node")
	}
}

// Compile-time check for unused import
var _ = runtime.RawExtension{}
