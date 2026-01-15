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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

// testConfig is a sample config structure for testing
type testConfig struct {
	Claim struct {
		Namespace string `json:"Namespace"`
		Name      string `json:"Name"`
	} `json:"Claim"`
	InterfaceName string `json:"InterfaceName"`
}

func TestStoreImplementations(t *testing.T) {
	// Test both implementations
	tests := []struct {
		name      string
		newStore  func(t *testing.T) Store
		cleanup   func(t *testing.T)
		storePath string
	}{
		{
			name: "memory",
			newStore: func(t *testing.T) Store {
				return New(Config{Type: TypeMemory})
			},
		},
		{
			name: "bbolt",
			newStore: func(t *testing.T) Store {
				path := filepath.Join(t.TempDir(), "test.db")
				return New(Config{Type: TypeBBolt, Path: path})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.newStore(t)
			if err := store.Open(); err != nil {
				t.Fatalf("failed to open store: %v", err)
			}
			defer store.Close()

			t.Run("PodNetNs", func(t *testing.T) {
				testPodNetNsOperations(t, store)
			})

			t.Run("PodConfigs", func(t *testing.T) {
				testPodConfigOperations(t, store)
			})
		})
	}
}

func testPodNetNsOperations(t *testing.T, store Store) {
	podKey := "default/test-pod"
	netNsPath := "/var/run/netns/test"

	// Test Set and Get
	if err := store.SetPodNetNs(podKey, netNsPath); err != nil {
		t.Fatalf("SetPodNetNs failed: %v", err)
	}

	got, ok := store.GetPodNetNs(podKey)
	if !ok {
		t.Fatal("GetPodNetNs returned not found")
	}
	if got != netNsPath {
		t.Errorf("GetPodNetNs = %q, want %q", got, netNsPath)
	}

	// Test GetPodNetNs for non-existent key
	_, ok = store.GetPodNetNs("nonexistent")
	if ok {
		t.Error("GetPodNetNs should return false for non-existent key")
	}

	// Test List
	list, err := store.ListPodNetNs()
	if err != nil {
		t.Fatalf("ListPodNetNs failed: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("ListPodNetNs returned %d items, want 1", len(list))
	}
	if list[podKey] != netNsPath {
		t.Errorf("ListPodNetNs[%q] = %q, want %q", podKey, list[podKey], netNsPath)
	}

	// Test Delete
	if err := store.DeletePodNetNs(podKey); err != nil {
		t.Fatalf("DeletePodNetNs failed: %v", err)
	}

	_, ok = store.GetPodNetNs(podKey)
	if ok {
		t.Error("GetPodNetNs should return false after delete")
	}
}

func testPodConfigOperations(t *testing.T, store Store) {
	podUID := types.UID("test-pod-uid-12345")
	deviceName := "eth0"

	config := testConfig{
		InterfaceName: "eth0",
	}
	config.Claim.Namespace = "default"
	config.Claim.Name = "test-claim"

	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	// Test Set and Get
	if err := store.SetPodConfig(podUID, deviceName, configBytes); err != nil {
		t.Fatalf("SetPodConfig failed: %v", err)
	}

	got, ok := store.GetPodConfig(podUID, deviceName)
	if !ok {
		t.Fatal("GetPodConfig returned not found")
	}

	var gotConfig testConfig
	if err := json.Unmarshal(got, &gotConfig); err != nil {
		t.Fatalf("failed to unmarshal config: %v", err)
	}
	if gotConfig.InterfaceName != config.InterfaceName {
		t.Errorf("InterfaceName = %q, want %q", gotConfig.InterfaceName, config.InterfaceName)
	}

	// Test GetPodConfigs
	configs, ok := store.GetPodConfigs(podUID)
	if !ok {
		t.Fatal("GetPodConfigs returned not found")
	}
	if len(configs) != 1 {
		t.Errorf("GetPodConfigs returned %d configs, want 1", len(configs))
	}

	// Add another device config
	device2 := "eth1"
	config2 := testConfig{InterfaceName: "eth1"}
	config2.Claim.Namespace = "default"
	config2.Claim.Name = "test-claim"
	config2Bytes, _ := json.Marshal(config2)
	if err := store.SetPodConfig(podUID, device2, config2Bytes); err != nil {
		t.Fatalf("SetPodConfig for second device failed: %v", err)
	}

	configs, ok = store.GetPodConfigs(podUID)
	if !ok {
		t.Fatal("GetPodConfigs returned not found after adding second device")
	}
	if len(configs) != 2 {
		t.Errorf("GetPodConfigs returned %d configs, want 2", len(configs))
	}

	// Test ListAllPodConfigs
	allConfigs, err := store.ListAllPodConfigs()
	if err != nil {
		t.Fatalf("ListAllPodConfigs failed: %v", err)
	}
	if len(allConfigs) != 1 {
		t.Errorf("ListAllPodConfigs returned %d pods, want 1", len(allConfigs))
	}
	if len(allConfigs[podUID]) != 2 {
		t.Errorf("ListAllPodConfigs returned %d configs for pod, want 2", len(allConfigs[podUID]))
	}

	// Test DeletePod
	if err := store.DeletePod(podUID); err != nil {
		t.Fatalf("DeletePod failed: %v", err)
	}

	_, ok = store.GetPodConfigs(podUID)
	if ok {
		t.Error("GetPodConfigs should return false after DeletePod")
	}

	// Test DeletePodConfigsByClaim
	// Re-add configs
	if err := store.SetPodConfig(podUID, deviceName, configBytes); err != nil {
		t.Fatalf("SetPodConfig failed: %v", err)
	}

	if err := store.DeletePodConfigsByClaim("default", "test-claim"); err != nil {
		t.Fatalf("DeletePodConfigsByClaim failed: %v", err)
	}

	_, ok = store.GetPodConfig(podUID, deviceName)
	if ok {
		t.Error("GetPodConfig should return false after DeletePodConfigsByClaim")
	}
}

func TestBBoltPersistence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")

	// Write data with first store instance
	store1 := New(Config{Type: TypeBBolt, Path: dbPath})
	if err := store1.Open(); err != nil {
		t.Fatalf("failed to open store1: %v", err)
	}

	podKey := "default/persistent-pod"
	netNsPath := "/var/run/netns/persistent"
	if err := store1.SetPodNetNs(podKey, netNsPath); err != nil {
		t.Fatalf("SetPodNetNs failed: %v", err)
	}

	podUID := types.UID("persistent-uid")
	config := []byte(`{"test": "data"}`)
	if err := store1.SetPodConfig(podUID, "eth0", config); err != nil {
		t.Fatalf("SetPodConfig failed: %v", err)
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("failed to close store1: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("database file was not created")
	}

	// Open new store instance and verify data persisted
	store2 := New(Config{Type: TypeBBolt, Path: dbPath})
	if err := store2.Open(); err != nil {
		t.Fatalf("failed to open store2: %v", err)
	}
	defer store2.Close()

	got, ok := store2.GetPodNetNs(podKey)
	if !ok {
		t.Fatal("GetPodNetNs returned not found after reopening")
	}
	if got != netNsPath {
		t.Errorf("GetPodNetNs = %q, want %q after reopening", got, netNsPath)
	}

	gotConfig, ok := store2.GetPodConfig(podUID, "eth0")
	if !ok {
		t.Fatal("GetPodConfig returned not found after reopening")
	}
	if string(gotConfig) != string(config) {
		t.Errorf("GetPodConfig = %q, want %q after reopening", gotConfig, config)
	}
}

func TestNewStoreDefaults(t *testing.T) {
	// Empty type should default to memory
	store := New(Config{})
	if _, ok := store.(*memoryStore); !ok {
		t.Error("New with empty config should return memoryStore")
	}

	// Explicit memory type
	store = New(Config{Type: TypeMemory})
	if _, ok := store.(*memoryStore); !ok {
		t.Error("New with TypeMemory should return memoryStore")
	}

	// Explicit bbolt type
	store = New(Config{Type: TypeBBolt, Path: "/tmp/test.db"})
	if _, ok := store.(*bboltStore); !ok {
		t.Error("New with TypeBBolt should return bboltStore")
	}
}
