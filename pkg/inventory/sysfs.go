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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
)

const (
	// https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-class-net
	sysnetPath = "/sys/class/net/"
	// Each of the entries in this directory is a symbolic link
	// representing one of the real or virtual networking devices
	// that are visible in the network namespace of the process
	// that is accessing the directory.  Each of these symbolic
	// links refers to entries in the /sys/devices directory.
	// https://man7.org/linux/man-pages/man5/sysfs.5.html
	sysdevPath = "/sys/devices"
)

// pciAddressRegex is used to identify a PCI address within a string.
// It matches patterns like "0000:00:04.0" or "00:04.0".
var pciAddressRegex = regexp.MustCompile(`^(?:([0-9a-fA-F]{4}):)?([0-9a-fA-F]{2}):([0-9a-fA-F]{2})\.([0-9a-fA-F])$`)

func realpath(ifName string, syspath string) string {
	linkPath := filepath.Join(syspath, ifName)
	dst, err := os.Readlink(linkPath)
	if err != nil {
		klog.Error(err, "unexpected error trying reading link", "link", linkPath)
	}
	var dstAbs string
	if filepath.IsAbs(dst) {
		dstAbs = dst
	} else {
		// Symlink targets are relative to the directory containing the link.
		dstAbs = filepath.Join(filepath.Dir(linkPath), dst)
	}
	return dstAbs
}

// $ realpath /sys/class/net/cilium_host
// /sys/devices/virtual/net/cilium_host
func isVirtual(name string, syspath string) bool {
	sysfsPath := realpath(name, syspath)
	prefix := filepath.Join(sysdevPath, "virtual")
	return strings.HasPrefix(sysfsPath, prefix)
}

func sriovTotalVFs(name string) int {
	totalVfsPath := filepath.Join(sysnetPath, name, "/device/sriov_totalvfs")
	totalBytes, err := os.ReadFile(totalVfsPath)
	if err != nil {
		klog.V(7).Infof("error trying to get total VFs for device %s: %v", name, err)
		return 0
	}
	total := bytes.TrimSpace(totalBytes)
	t, err := strconv.Atoi(string(total))
	if err != nil {
		klog.Errorf("Error in obtaining maximum supported number of virtual functions for network interface: %s: %v", name, err)
		return 0
	}
	return t
}

func sriovNumVFs(name string) int {
	numVfsPath := filepath.Join(sysnetPath, name, "/device/sriov_numvfs")
	numBytes, err := os.ReadFile(numVfsPath)
	if err != nil {
		klog.V(7).Infof("error trying to get number of VFs for device %s: %v", name, err)
		return 0
	}
	num := bytes.TrimSpace(numBytes)
	t, err := strconv.Atoi(string(num))
	if err != nil {
		klog.Errorf("Error in obtaining number of virtual functions for network interface: %s: %v", name, err)
		return 0
	}
	return t
}

// getRDMADeviceNameFromSysfs returns the RDMA device name for a network interface
// by examining the sysfs infiniband directory.
func getRDMADeviceNameFromSysfs(ifName string) string {
	ibPath := filepath.Join(sysnetPath, ifName, "device", "infiniband")
	entries, err := os.ReadDir(ibPath)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return entry.Name()
		}
	}
	return ""
}

// infinibandDevPath is the path to the infiniband device directory.
// It can be overridden for testing.
var infinibandDevPath = "/dev/infiniband"

// ListAllUverbsDevices returns all uverbs character devices found in /dev/infiniband/.
// This is useful for environments where uverbs devices exist but rdmamap cannot
// map them to specific RDMA devices (e.g., mock/test environments).
func ListAllUverbsDevices() []string {
	return listUverbsDevicesInDir(infinibandDevPath)
}

// listUverbsDevicesInDir lists uverbs devices in the specified directory.
func listUverbsDevicesInDir(dirPath string) []string {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		klog.V(4).Infof("Could not read %s: %v", dirPath, err)
		return nil
	}

	var uverbsDevs []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "uverbs") {
			uverbsDevs = append(uverbsDevs, filepath.Join(dirPath, entry.Name()))
		}
	}
	return uverbsDevs
}

// sysClassInfinibandPath is the sysfs path for RDMA/InfiniBand devices.
// Can be overridden for testing.
var sysClassInfinibandPath = "/sys/class/infiniband"

// RDMADeviceInfo contains information about an RDMA device discovered from sysfs.
type RDMADeviceInfo struct {
	// Name is the RDMA device name (e.g., "mlx5_0")
	Name string
	// NodeGUID is the InfiniBand node GUID
	NodeGUID string
	// NodeType is the device type: "ca" (channel adapter), "switch", "router", etc.
	NodeType string
	// NumPorts is the number of ports on this RDMA device
	NumPorts int
	// Ports contains per-port information
	Ports []RDMAPortInfo
	// PCIAddress is the PCI address if available
	PCIAddress string
	// FirmwareVersion is the device firmware version
	FirmwareVersion string
}

// RDMAPortInfo contains information about a specific port on an RDMA device.
type RDMAPortInfo struct {
	// PortNum is the 1-based port number
	PortNum int
	// State is the port state (e.g., "ACTIVE", "DOWN")
	State string
	// PhysState is the physical state (e.g., "LinkUp", "Polling")
	PhysState string
	// LinkLayer is the link layer type: "InfiniBand" or "Ethernet" (RoCE)
	LinkLayer string
	// NetDev is the associated network interface name (if any)
	NetDev string
	// GID is the port GID (for IPoIB)
	GID string
}

// ListRDMADevices discovers all RDMA devices from /sys/class/infiniband.
// This allows discovering RDMA devices that may not have associated network interfaces.
func ListRDMADevices() ([]RDMADeviceInfo, error) {
	return listRDMADevicesInDir(sysClassInfinibandPath)
}

// listRDMADevicesInDir discovers RDMA devices from a specified sysfs path.
func listRDMADevicesInDir(sysPath string) ([]RDMADeviceInfo, error) {
	entries, err := os.ReadDir(sysPath)
	if err != nil {
		if os.IsNotExist(err) {
			klog.V(4).Infof("No RDMA devices found: %s does not exist", sysPath)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read %s: %w", sysPath, err)
	}

	var devices []RDMADeviceInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		devName := entry.Name()
		devPath := filepath.Join(sysPath, devName)

		info := RDMADeviceInfo{
			Name: devName,
		}

		// Read node_guid
		if data, err := os.ReadFile(filepath.Join(devPath, "node_guid")); err == nil {
			info.NodeGUID = strings.TrimSpace(string(data))
		}

		// Read node_type
		if data, err := os.ReadFile(filepath.Join(devPath, "node_type")); err == nil {
			info.NodeType = strings.TrimSpace(string(data))
		}

		// Read fw_ver
		if data, err := os.ReadFile(filepath.Join(devPath, "fw_ver")); err == nil {
			info.FirmwareVersion = strings.TrimSpace(string(data))
		}

		// Get PCI address from device symlink
		if deviceLink, err := os.Readlink(filepath.Join(devPath, "device")); err == nil {
			if pciAddr, err := pciAddressFromPath(deviceLink); err == nil {
				info.PCIAddress = pciAddr.String()
			}
		}

		// Discover ports
		portsPath := filepath.Join(devPath, "ports")
		portEntries, err := os.ReadDir(portsPath)
		if err == nil {
			for _, portEntry := range portEntries {
				portNum, err := strconv.Atoi(portEntry.Name())
				if err != nil {
					continue
				}
				portPath := filepath.Join(portsPath, portEntry.Name())
				portInfo := RDMAPortInfo{PortNum: portNum}

				// Read state
				if data, err := os.ReadFile(filepath.Join(portPath, "state")); err == nil {
					// Format: "4: ACTIVE" - extract just the state name
					parts := strings.SplitN(strings.TrimSpace(string(data)), ":", 2)
					if len(parts) == 2 {
						portInfo.State = strings.TrimSpace(parts[1])
					} else {
						portInfo.State = strings.TrimSpace(string(data))
					}
				}

				// Read phys_state
				if data, err := os.ReadFile(filepath.Join(portPath, "phys_state")); err == nil {
					parts := strings.SplitN(strings.TrimSpace(string(data)), ":", 2)
					if len(parts) == 2 {
						portInfo.PhysState = strings.TrimSpace(parts[1])
					} else {
						portInfo.PhysState = strings.TrimSpace(string(data))
					}
				}

				// Read link_layer (InfiniBand vs Ethernet/RoCE)
				if data, err := os.ReadFile(filepath.Join(portPath, "link_layer")); err == nil {
					portInfo.LinkLayer = strings.TrimSpace(string(data))
				}

				// Find associated netdev by checking gid_attrs/ndevs
				ndevsPath := filepath.Join(portPath, "gid_attrs", "ndevs")
				if ndevEntries, err := os.ReadDir(ndevsPath); err == nil && len(ndevEntries) > 0 {
					// Read the first ndev entry
					if data, err := os.ReadFile(filepath.Join(ndevsPath, ndevEntries[0].Name())); err == nil {
						portInfo.NetDev = strings.TrimSpace(string(data))
					}
				}

				// Alternative: check via gids for the netdev
				if portInfo.NetDev == "" {
					gidsPath := filepath.Join(portPath, "gids")
					if gidEntries, err := os.ReadDir(gidsPath); err == nil && len(gidEntries) > 0 {
						if data, err := os.ReadFile(filepath.Join(gidsPath, gidEntries[0].Name())); err == nil {
							portInfo.GID = strings.TrimSpace(string(data))
						}
					}
				}

				info.Ports = append(info.Ports, portInfo)
			}
		}
		info.NumPorts = len(info.Ports)

		devices = append(devices, info)
	}

	return devices, nil
}

// GetUverbsForRDMADevice returns the uverbs device path for an RDMA device
// by looking up /sys/class/infiniband/<dev>/device/infiniband_verbs/.
func GetUverbsForRDMADevice(rdmaDevName string) string {
	verbsPath := filepath.Join(sysClassInfinibandPath, rdmaDevName, "device", "infiniband_verbs")
	entries, err := os.ReadDir(verbsPath)
	if err != nil {
		return ""
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "uverbs") {
			return filepath.Join(infinibandDevPath, entry.Name())
		}
	}
	return ""
}

// pciAddress BDF Notation
// [domain:]bus:device.function
// https://wiki.xenproject.org/wiki/Bus:Device.Function_(BDF)_Notation
type pciAddress struct {
	// There might be several independent sets of PCI devices
	// (e.g. several host PCI controllers on a mainboard chipset)
	domain string
	bus    string
	device string
	// One PCI device (e.g. pluggable card) may implement several functions
	// (e.g. sound card and joystick controller used to be a common combo),
	// so PCI provides for up to 8 separate functions on a single PCI device.
	function string
}

func (a pciAddress) String() string {
	if a.domain == "" {
		return fmt.Sprintf("%s:%s.%s", a.bus, a.device, a.function)
	}
	return fmt.Sprintf("%s:%s:%s.%s", a.domain, a.bus, a.device, a.function)
}

// The PCI root is the root PCI device, derived from the
// pciAddress of a device. Spec is defined from the DRA KEP.
// https://github.com/kubernetes/enhancements/pull/5316
type pciRoot struct {
	domain string
	// The root may have a different host bus than the PCI device.
	// e.g https://uefi.org/specs/UEFI/2.10/14_Protocols_PCI_Bus_Support.html#server-system-with-four-pci-root-bridges
	bus string
}

// parsePCIAddress takes a string and attempts to extract and parse a PCI address from it.
func parsePCIAddress(s string) (*pciAddress, error) {
	matches := pciAddressRegex.FindStringSubmatch(s)
	if matches == nil {
		return nil, fmt.Errorf("could not find PCI address in string: %s", s)
	}
	address := &pciAddress{}

	// When pciAddressRegex matches, it is expected to return 5 elements. (First
	// is the complete matched string itself, and the next 4 are the submatches
	// corresponding to Domain:Bus:Device.Function). Examples:
	// 	- "0000:00:04.0" -> ["0000:00:04.0" "0000" "00" "04" "0"]
	// 	- "00:05.0" -> ["0000:00:05.0" "" "00" "05" "0"]
	if len(matches) == 5 {
		address.domain = matches[1]
		address.bus = matches[2]
		address.device = matches[3]
		address.function = matches[4]
	} else {
		return nil, fmt.Errorf("invalid PCI address format: %s", s)
	}

	return address, nil
}

// pciAddressFromPath takes a full sysfs path and traverses it upwards to find
// the first component that contains a valid PCI address.
func pciAddressFromPath(path string) (*pciAddress, error) {
	parts := strings.Split(path, "/")
	for len(parts) > 0 {
		current := parts[len(parts)-1]
		addr, err := parsePCIAddress(current)
		if err == nil {
			return addr, nil
		}
		parts = parts[:len(parts)-1]
	}
	return nil, fmt.Errorf("could not find PCI address in path: %s", path)
}

// pciAddressForNetInterface finds the PCI address for a given network interface name.
func pciAddressForNetInterface(ifName string) (*pciAddress, error) {
	// First, find the absolute path of the device in the sysfs, which typically
	// looks like:
	// /sys/devices/pci0000:8c/0000:8c:00.0/0000:8d:00.0/0000:8e:02.0/0000:91:00.0/net/eth0
	// Then, use pciAddressFromPath() to traverse the path upwards, checking
	// each component to find the first one that matches the format of a PCI
	// address.
	sysfsPath := realpath(ifName, sysnetPath)
	address, err := pciAddressFromPath(sysfsPath)
	if err != nil {
		return nil, fmt.Errorf("could not find PCI address for interface %q: %w", ifName, err)
	}
	return address, nil
}
