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

package inventory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetRDMADeviceNameFromSysfs(t *testing.T) {
	// This test would require creating a fake sysfs structure
	// For now, just test that it returns empty string for non-existent paths
	got := getRDMADeviceNameFromSysfs("nonexistent_interface_12345")
	if got != "" {
		t.Errorf("getRDMADeviceNameFromSysfs(nonexistent) = %q, want empty string", got)
	}
}

func TestListAllUverbsDevices(t *testing.T) {
	// This test uses the real /dev/infiniband directory if it exists
	devices := ListAllUverbsDevices()
	t.Logf("Found %d uverbs devices in /dev/infiniband: %v", len(devices), devices)

	// Verify all returned devices have the uverbs prefix
	for _, dev := range devices {
		if !strings.Contains(dev, "uverbs") {
			t.Errorf("ListAllUverbsDevices() returned non-uverbs device: %s", dev)
		}
		if !strings.HasPrefix(dev, "/dev/infiniband/") {
			t.Errorf("ListAllUverbsDevices() returned device without correct path prefix: %s", dev)
		}
	}
}

func TestListUverbsDevicesInDir_WithMockDir(t *testing.T) {
	// Create a temporary directory with mock uverbs files
	tempDir := t.TempDir()

	// Create mock uverbs files (like what you'd see on a real system)
	mockDevices := []string{"uverbs0", "uverbs1", "uverbs2", "uverbs3", "uverbs4", "uverbs5", "uverbs6", "uverbs7", "uverbs8"}
	for _, dev := range mockDevices {
		f, err := os.Create(filepath.Join(tempDir, dev))
		if err != nil {
			t.Fatalf("Failed to create mock device %s: %v", dev, err)
		}
		f.Close()
	}

	// Also create some non-uverbs files that should be ignored
	for _, name := range []string{"rdma_cm", "issm0", "umad0"} {
		f, err := os.Create(filepath.Join(tempDir, name))
		if err != nil {
			t.Fatalf("Failed to create mock file %s: %v", name, err)
		}
		f.Close()
	}

	// Test the function
	devices := listUverbsDevicesInDir(tempDir)

	if len(devices) != len(mockDevices) {
		t.Errorf("listUverbsDevicesInDir() returned %d devices, want %d", len(devices), len(mockDevices))
	}

	// Verify each expected device is present
	for _, expected := range mockDevices {
		expectedPath := filepath.Join(tempDir, expected)
		found := false
		for _, dev := range devices {
			if dev == expectedPath {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("listUverbsDevicesInDir() did not return expected device %s", expectedPath)
		}
	}

	// Verify no non-uverbs devices were returned
	for _, dev := range devices {
		if !strings.Contains(dev, "uverbs") {
			t.Errorf("listUverbsDevicesInDir() returned non-uverbs device: %s", dev)
		}
	}
}

func TestListRDMADevices_WithMockSysfs(t *testing.T) {
	// Create a mock sysfs structure
	tempDir := t.TempDir()

	// Create mock RDMA devices
	mockDevices := []struct {
		name     string
		nodeGUID string
		nodeType string
		fwVer    string
		ports    []struct {
			num       string
			state     string
			physState string
			linkLayer string
		}
	}{
		{
			name:     "mlx5_0",
			nodeGUID: "0015:5dff:fe33:ff13",
			nodeType: "1: CA",
			fwVer:    "20.41.1854",
			ports: []struct {
				num       string
				state     string
				physState string
				linkLayer string
			}{
				{"1", "4: ACTIVE", "5: LinkUp", "InfiniBand"},
			},
		},
		{
			name:     "mlx5_1",
			nodeGUID: "6045:bdff:fec5:50f2",
			nodeType: "1: CA",
			fwVer:    "14.30.5026",
			ports: []struct {
				num       string
				state     string
				physState string
				linkLayer string
			}{
				{"1", "4: ACTIVE", "5: LinkUp", "Ethernet"},
				{"2", "1: DOWN", "3: Disabled", "Ethernet"},
			},
		},
	}

	for _, dev := range mockDevices {
		devPath := filepath.Join(tempDir, dev.name)
		if err := os.MkdirAll(devPath, 0755); err != nil {
			t.Fatalf("Failed to create device dir: %v", err)
		}

		// Write node_guid
		if err := os.WriteFile(filepath.Join(devPath, "node_guid"), []byte(dev.nodeGUID+"\n"), 0644); err != nil {
			t.Fatalf("Failed to write node_guid: %v", err)
		}

		// Write node_type
		if err := os.WriteFile(filepath.Join(devPath, "node_type"), []byte(dev.nodeType+"\n"), 0644); err != nil {
			t.Fatalf("Failed to write node_type: %v", err)
		}

		// Write fw_ver
		if err := os.WriteFile(filepath.Join(devPath, "fw_ver"), []byte(dev.fwVer+"\n"), 0644); err != nil {
			t.Fatalf("Failed to write fw_ver: %v", err)
		}

		// Create ports
		for _, port := range dev.ports {
			portPath := filepath.Join(devPath, "ports", port.num)
			if err := os.MkdirAll(portPath, 0755); err != nil {
				t.Fatalf("Failed to create port dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(portPath, "state"), []byte(port.state+"\n"), 0644); err != nil {
				t.Fatalf("Failed to write state: %v", err)
			}
			if err := os.WriteFile(filepath.Join(portPath, "phys_state"), []byte(port.physState+"\n"), 0644); err != nil {
				t.Fatalf("Failed to write phys_state: %v", err)
			}
			if err := os.WriteFile(filepath.Join(portPath, "link_layer"), []byte(port.linkLayer+"\n"), 0644); err != nil {
				t.Fatalf("Failed to write link_layer: %v", err)
			}
		}
	}

	// Test the function
	devices, err := listRDMADevicesInDir(tempDir)
	if err != nil {
		t.Fatalf("listRDMADevicesInDir() returned error: %v", err)
	}

	if len(devices) != len(mockDevices) {
		t.Errorf("listRDMADevicesInDir() returned %d devices, want %d", len(devices), len(mockDevices))
	}

	// Verify first device
	var mlx5_0 *RDMADeviceInfo
	for i := range devices {
		if devices[i].Name == "mlx5_0" {
			mlx5_0 = &devices[i]
			break
		}
	}
	if mlx5_0 == nil {
		t.Fatal("mlx5_0 not found")
	}

	if mlx5_0.NodeGUID != "0015:5dff:fe33:ff13" {
		t.Errorf("mlx5_0.NodeGUID = %q, want %q", mlx5_0.NodeGUID, "0015:5dff:fe33:ff13")
	}
	if mlx5_0.FirmwareVersion != "20.41.1854" {
		t.Errorf("mlx5_0.FirmwareVersion = %q, want %q", mlx5_0.FirmwareVersion, "20.41.1854")
	}
	if mlx5_0.NumPorts != 1 {
		t.Errorf("mlx5_0.NumPorts = %d, want 1", mlx5_0.NumPorts)
	}
	if len(mlx5_0.Ports) != 1 {
		t.Fatalf("mlx5_0 has %d ports, want 1", len(mlx5_0.Ports))
	}
	if mlx5_0.Ports[0].State != "ACTIVE" {
		t.Errorf("mlx5_0.Ports[0].State = %q, want %q", mlx5_0.Ports[0].State, "ACTIVE")
	}
	if mlx5_0.Ports[0].LinkLayer != "InfiniBand" {
		t.Errorf("mlx5_0.Ports[0].LinkLayer = %q, want %q", mlx5_0.Ports[0].LinkLayer, "InfiniBand")
	}

	// Verify second device has 2 ports
	var mlx5_1 *RDMADeviceInfo
	for i := range devices {
		if devices[i].Name == "mlx5_1" {
			mlx5_1 = &devices[i]
			break
		}
	}
	if mlx5_1 == nil {
		t.Fatal("mlx5_1 not found")
	}
	if mlx5_1.NumPorts != 2 {
		t.Errorf("mlx5_1.NumPorts = %d, want 2", mlx5_1.NumPorts)
	}
}
