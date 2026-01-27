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
	"context"
	"fmt"
	"maps"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/dranet/pkg/apis"
	"github.com/google/dranet/pkg/cloudprovider"
	"github.com/google/dranet/pkg/names"

	"github.com/Mellanox/rdmamap"
	"github.com/google/dranet/internal/nlwrap"
	"github.com/jaypipes/ghw"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/time/rate"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/deviceattribute"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	// defaultMinPollInterval is the default minimum interval between two
	// consecutive polls of the inventory.
	defaultMinPollInterval = 2 * time.Second
	// defaultMaxPollInterval is the default maximum interval between two
	// consecutive polls of the inventory.
	defaultMaxPollInterval = 1 * time.Minute
	// defaultPollBurst is the default number of polls that can be run in a
	// burst.
	defaultPollBurst = 5
)

var (
	// ignoredInterfaceNames is a set of network interface names that are typically
	// created by CNI plugins or are otherwise not relevant for DRA resource exposure.
	ignoredInterfaceNames = sets.New("cilium_net", "cilium_host", "docker0")
)

type DB struct {
	instance *cloudprovider.CloudInstance
	// TODO: it is not common but may happen in edge cases that the default
	// gateway changes revisit once we have more evidence this can be a
	// potential problem or break some use cases.
	gwInterfaces sets.Set[string]

	mu sync.RWMutex
	// podNetNsStore gives the network namespace for a pod, indexed by the pods
	// "namespaced/name".
	podNetNsStore map[string]string
	// deviceStore is an in-memory cache of the available devices on the node.
	// It is keyed by the normalized PCI address of the device. The value is a
	// resourceapi.Device object that contains the device's attributes.
	// The deviceStore is periodically updated by the Run method.
	deviceStore map[string]resourceapi.Device

	rateLimiter     *rate.Limiter
	maxPollInterval time.Duration
	notifications   chan []resourceapi.Device
	hasDevices      bool
}

type Option func(*DB)

func WithRateLimiter(limiter *rate.Limiter) Option {
	return func(db *DB) {
		db.rateLimiter = limiter
	}
}

func WithMaxPollInterval(d time.Duration) Option {
	return func(db *DB) {
		db.maxPollInterval = d
	}
}

func New(opts ...Option) *DB {
	db := &DB{
		podNetNsStore:   map[string]string{},
		deviceStore:     map[string]resourceapi.Device{},
		rateLimiter:     rate.NewLimiter(rate.Every(defaultMinPollInterval), defaultPollBurst),
		notifications:   make(chan []resourceapi.Device),
		maxPollInterval: defaultMaxPollInterval,
	}
	for _, o := range opts {
		o(db)
	}
	return db
}

func (db *DB) AddPodNetNs(pod string, netNsPath string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	ns, err := netns.GetFromPath(netNsPath)
	if err != nil {
		klog.Errorf("Failed to get pod %s network namespace %s handle: %v", pod, netNsPath, err)
		return
	}
	defer ns.Close()
	db.podNetNsStore[pod] = netNsPath
}

func (db *DB) RemovePodNetNs(pod string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.podNetNsStore, pod)
}

// GetPodNamespace allows to get the Pod network namespace
func (db *DB) GetPodNetNs(pod string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.podNetNsStore[pod]
}

func (db *DB) Run(ctx context.Context) error {
	defer close(db.notifications)

	// Resources are published periodically or if there is a netlink notification
	// indicating a new interfaces was added or changed
	nlChannel := make(chan netlink.LinkUpdate)
	doneCh := make(chan struct{})
	defer close(doneCh)
	if err := netlink.LinkSubscribe(nlChannel, doneCh); err != nil {
		klog.Error(err, "error subscribing to netlink interfaces, only syncing periodically", "interval", db.maxPollInterval.String())
	}

	// Obtain data that will not change after the startup
	db.instance = getInstanceProperties(ctx)
	db.gwInterfaces = getDefaultGwInterfaces()
	klog.V(2).Infof("Default gateway interfaces: %v", db.gwInterfaces.UnsortedList())

	for {
		err := db.rateLimiter.Wait(ctx)
		if err != nil {
			klog.Error(err, "unexpected rate limited error trying to get system interfaces")
		}

		filteredDevices := db.scan()
		if len(filteredDevices) > 0 || db.hasDevices {
			db.hasDevices = len(filteredDevices) > 0
			db.notifications <- filteredDevices
		}

		select {
		// trigger a reconcile
		case <-nlChannel:
			// drain the channel so we only sync once
			for len(nlChannel) > 0 {
				<-nlChannel
			}
		case <-time.After(db.maxPollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// scan discovers the available devices on the node.
// It discovers PCI, network, and RDMA devices, adds cloud attributes,
// filters out default interfaces, and updates the device store.
func (db *DB) scan() []resourceapi.Device {
	devices := db.discoverPCIDevices()
	devices = db.discoverNetworkInterfaces(devices)
	devices = db.discoverRDMADevices(devices)
	devices = db.addCloudAttributes(devices)

	// Remove default interface.
	filteredDevices := []resourceapi.Device{}
	for _, device := range devices {
		ifName := device.Attributes[apis.AttrInterfaceName].StringValue
		if ifName != nil && db.gwInterfaces.Has(string(*ifName)) {
			klog.V(4).Infof("Ignoring interface %s from discovery since it is an uplink interface", *ifName)
			continue
		}
		filteredDevices = append(filteredDevices, device)
	}

	sort.Slice(filteredDevices, func(i, j int) bool {
		return filteredDevices[i].Name < filteredDevices[j].Name
	})

	klog.V(4).Infof("Found %d devices", len(filteredDevices))
	db.updateDeviceStore(filteredDevices)
	return filteredDevices
}

func (db *DB) GetResources(ctx context.Context) <-chan []resourceapi.Device {
	return db.notifications
}

func (db *DB) discoverPCIDevices() []resourceapi.Device {
	devices := []resourceapi.Device{}

	pci, err := ghw.PCI(
		ghw.WithDisableTools(),
	)
	if err != nil {
		klog.Errorf("Could not get PCI devices: %v", err)
		return devices
	}

	for _, pciDev := range pci.Devices {
		if !isNetworkDevice(pciDev) {
			continue
		}
		device := resourceapi.Device{
			Name:       names.NormalizePCIAddress(pciDev.Address),
			Attributes: make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute),
			Capacity:   make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity),
		}
		device.Attributes[apis.AttrPCIAddress] = resourceapi.DeviceAttribute{StringValue: &pciDev.Address}
		if pciDev.Vendor != nil {
			device.Attributes[apis.AttrPCIVendor] = resourceapi.DeviceAttribute{StringValue: ptr.To(truncateString(pciDev.Vendor.Name, 64))}
		}
		if pciDev.Product != nil {
			device.Attributes[apis.AttrPCIDevice] = resourceapi.DeviceAttribute{StringValue: ptr.To(truncateString(pciDev.Product.Name, 64))}
		}
		if pciDev.Subsystem != nil {
			device.Attributes[apis.AttrPCISubsystem] = resourceapi.DeviceAttribute{StringValue: &pciDev.Subsystem.ID}
		}

		if pciDev.Node != nil {
			device.Attributes[apis.AttrNUMANode] = resourceapi.DeviceAttribute{IntValue: ptr.To(int64(pciDev.Node.ID))}
		}

		pcieRootAttr, err := deviceattribute.GetPCIeRootAttributeByPCIBusID(pciDev.Address)
		if err != nil {
			klog.Infof("Could not get pci root attribute: %v", err)
		} else {
			device.Attributes[pcieRootAttr.Name] = pcieRootAttr.Value
		}
		devices = append(devices, device)
	}
	return devices
}

// discoveryNetworkInterfaces updates the devices based on information retried
// from network interfaces. For each network interface, the two possible
// outcomes are:
//   - If the network interface is associated with some parent PCI device, the
//     existing PCI device is modified with additional attributes related to the
//     network interface.
//   - For Network interfaces which are not associated with a PCI Device (like
//     virtual interfaces), they are added as their own device.
func (db *DB) discoverNetworkInterfaces(pciDevices []resourceapi.Device) []resourceapi.Device {
	links, err := nlwrap.LinkList()
	if err != nil {
		klog.Errorf("Could not list network interfaces: %v", err)
		return pciDevices
	}

	pciDeviceMap := make(map[string]*resourceapi.Device)
	for i := range pciDevices {
		pciDeviceMap[pciDevices[i].Name] = &pciDevices[i]
	}

	otherDevices := []resourceapi.Device{}

	for _, link := range links {
		ifName := link.Attrs().Name
		if ignoredInterfaceNames.Has(ifName) {
			klog.V(4).Infof("Network Interface %s is in the list of ignored interfaces, excluding it from discovery", ifName)
			continue
		}

		// skip loopback interfaces
		if link.Attrs().Flags&net.FlagLoopback != 0 {
			klog.V(4).Infof("Network Interface %s is a loopback interface, excluding it from discovery", ifName)
			continue
		}

		pciAddr, err := pciAddressForNetInterface(ifName)
		if err == nil {
			// It's a PCI device.

			normalizedAddress := names.NormalizePCIAddress(pciAddr.String())
			var exists bool
			device, exists := pciDeviceMap[normalizedAddress]
			if !exists {
				// We don't expect this to happen.
				klog.Errorf("Network interface %s has PCI address %q, but it was not found in initial PCI scan.", ifName, pciAddr)
				continue
			}
			addLinkAttributes(device, link)
		} else {
			// Not a PCI device.

			if !isVirtual(ifName, sysnetPath) {
				// If we failed to identify the PCI address of the network
				// interface and the network interface is also not a virtual
				// device, use a best-effort strategy where the network
				// interface is assumed to be virtual.
				klog.Warningf("PCI address not found for non-virtual interface %s, proceeding as if it were virtual. Error: %v", ifName, err)
			}
			newDevice := &resourceapi.Device{
				Name:       names.NormalizeInterfaceName(ifName),
				Attributes: make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute),
			}
			addLinkAttributes(newDevice, link)
			otherDevices = append(otherDevices, *newDevice)
		}
	}

	return append(pciDevices, otherDevices...)
}

func addLinkAttributes(device *resourceapi.Device, link netlink.Link) {
	ifName := link.Attrs().Name
	device.Attributes[apis.AttrInterfaceName] = resourceapi.DeviceAttribute{StringValue: &ifName}
	device.Attributes[apis.AttrMac] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().HardwareAddr.String())}
	device.Attributes[apis.AttrMTU] = resourceapi.DeviceAttribute{IntValue: ptr.To(int64(link.Attrs().MTU))}
	device.Attributes[apis.AttrEncapsulation] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().EncapType)}
	device.Attributes[apis.AttrAlias] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().Alias)}
	device.Attributes[apis.AttrState] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().OperState.String())}
	device.Attributes[apis.AttrType] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Type())}

	v4 := sets.Set[string]{}
	v6 := sets.Set[string]{}
	if ips, err := nlwrap.AddrList(link, netlink.FAMILY_ALL); err == nil && len(ips) > 0 {
		for _, address := range ips {
			if !address.IP.IsGlobalUnicast() {
				continue
			}

			if address.IP.To4() == nil && address.IP.To16() != nil {
				v6.Insert(address.IPNet.String())
			} else if address.IP.To4() != nil {
				v4.Insert(address.IPNet.String())
			}
		}
		if v4.Len() > 0 {
			device.Attributes[apis.AttrIPv4] = resourceapi.DeviceAttribute{StringValue: ptr.To(strings.Join(v4.UnsortedList(), ","))}
		}
		if v6.Len() > 0 {
			device.Attributes[apis.AttrIPv6] = resourceapi.DeviceAttribute{StringValue: ptr.To(strings.Join(v6.UnsortedList(), ","))}
		}
	}

	isEbpf := false
	filterNames, ok := getTcFilters(link)
	if ok {
		isEbpf = true
		device.Attributes[apis.AttrTCFilterNames] = resourceapi.DeviceAttribute{StringValue: ptr.To(strings.Join(filterNames, ","))}
	}

	programNames, ok := getTcxFilters(link)
	if ok {
		isEbpf = true
		device.Attributes[apis.AttrTCXProgramNames] = resourceapi.DeviceAttribute{StringValue: ptr.To(strings.Join(programNames, ","))}
	}
	device.Attributes[apis.AttrEBPF] = resourceapi.DeviceAttribute{BoolValue: &isEbpf}

	isSRIOV := sriovTotalVFs(ifName) > 0
	device.Attributes[apis.AttrSRIOV] = resourceapi.DeviceAttribute{BoolValue: &isSRIOV}
	if isSRIOV {
		vfs := int64(sriovNumVFs(ifName))
		device.Attributes[apis.AttrSRIOVVfs] = resourceapi.DeviceAttribute{IntValue: &vfs}
	}

	if isVirtual(ifName, sysnetPath) {
		device.Attributes[apis.AttrVirtual] = resourceapi.DeviceAttribute{BoolValue: ptr.To(true)}
	} else {
		device.Attributes[apis.AttrVirtual] = resourceapi.DeviceAttribute{BoolValue: ptr.To(false)}
	}
}

func (db *DB) discoverRDMADevices(devices []resourceapi.Device) []resourceapi.Device {
	// Build indexes of devices we already know about for quick lookup
	pciToDeviceIdx := make(map[string]int)
	netdevToDeviceIdx := make(map[string]int)
	for i := range devices {
		if pciAddr := devices[i].Attributes[apis.AttrPCIAddress].StringValue; pciAddr != nil {
			pciToDeviceIdx[*pciAddr] = i
		}
		if ifName := devices[i].Attributes[apis.AttrInterfaceName].StringValue; ifName != nil {
			netdevToDeviceIdx[*ifName] = i
		}
	}

	// Enrich existing devices with RDMA attributes
	for i := range devices {
		rdmaDevName := getRDMADeviceNameForDevice(&devices[i])
		isRDMA := rdmaDevName != ""
		devices[i].Attributes[apis.AttrRDMA] = resourceapi.DeviceAttribute{BoolValue: &isRDMA}

		if isRDMA {
			addRDMAAttributes(&devices[i], rdmaDevName)
		}
	}

	// Discover RDMA-only devices (those without network interfaces)
	rdmaOnlyDevices := db.discoverRDMAOnlyDevices(pciToDeviceIdx, netdevToDeviceIdx)
	return append(devices, rdmaOnlyDevices...)
}

// getRDMADeviceNameForDevice determines the RDMA device name associated with
// a given device. It checks network interfaces first, then falls back to PCI address.
// Returns empty string if no RDMA device is found.
func getRDMADeviceNameForDevice(device *resourceapi.Device) string {
	// Try to find RDMA device by network interface name
	if ifName := device.Attributes[apis.AttrInterfaceName].StringValue; ifName != nil && *ifName != "" {
		// Try rdmamap library first
		if rdmaDev, _ := rdmamap.GetRdmaDeviceForNetdevice(*ifName); rdmaDev != "" {
			return rdmaDev
		}

		// Fallback to sysfs. This is needed for InfiniBand interfaces where
		// rdmamap has a bug comparing node GUID instead of port GUID:
		// https://github.com/Mellanox/rdmamap/issues/15
		if rdmaDev := getRDMADeviceNameFromSysfs(*ifName); rdmaDev != "" {
			return rdmaDev
		}
	}

	// Try to find RDMA device by PCI address
	if pciAddr := device.Attributes[apis.AttrPCIAddress].StringValue; pciAddr != nil && *pciAddr != "" {
		if rdmaDevices := rdmamap.GetRdmaDevicesForPcidev(*pciAddr); len(rdmaDevices) > 0 {
			return rdmaDevices[0]
		}
	}

	return ""
}

// addRDMAAttributes adds RDMA-specific attributes to a device.
func addRDMAAttributes(device *resourceapi.Device, rdmaDevName string) {
	device.Attributes[apis.AttrRDMADeviceName] = resourceapi.DeviceAttribute{
		StringValue: ptr.To(rdmaDevName),
	}
	if uverbsPath := GetUverbsForRDMADevice(rdmaDevName); uverbsPath != "" {
		device.Attributes[apis.AttrRDMAUverbsDev] = resourceapi.DeviceAttribute{
			StringValue: ptr.To(uverbsPath),
		}
	}
}

// discoverRDMAOnlyDevices finds RDMA devices that don't have an associated
// network interface (pure InfiniBand devices without IPoIB).
func (db *DB) discoverRDMAOnlyDevices(pciToDeviceIdx, netdevToDeviceIdx map[string]int) []resourceapi.Device {
	rdmaDevices, err := ListRDMADevices()
	if err != nil {
		klog.V(4).Infof("Failed to list RDMA devices from sysfs: %v", err)
		return nil
	}

	var devices []resourceapi.Device
	for _, rdmaInfo := range rdmaDevices {
		if isRDMADeviceAlreadyTracked(rdmaInfo, pciToDeviceIdx, netdevToDeviceIdx) {
			continue
		}

		klog.V(4).Infof("Found RDMA-only device: %s (PCI: %s, type: %s)", rdmaInfo.Name, rdmaInfo.PCIAddress, rdmaInfo.NodeType)
		devices = append(devices, createRDMAOnlyDevice(rdmaInfo))
	}
	return devices
}

// isRDMADeviceAlreadyTracked checks if an RDMA device is already represented
// by an existing device entry (either by PCI address or network interface).
func isRDMADeviceAlreadyTracked(rdmaInfo RDMADeviceInfo, pciToDeviceIdx, netdevToDeviceIdx map[string]int) bool {
	if rdmaInfo.PCIAddress != "" {
		if _, exists := pciToDeviceIdx[rdmaInfo.PCIAddress]; exists {
			return true
		}
	}
	for _, port := range rdmaInfo.Ports {
		if port.NetDev != "" {
			if _, exists := netdevToDeviceIdx[port.NetDev]; exists {
				return true
			}
		}
	}
	return false
}

// createRDMAOnlyDevice creates a new device entry for an RDMA-only device.
func createRDMAOnlyDevice(rdmaInfo RDMADeviceInfo) resourceapi.Device {
	device := resourceapi.Device{
		Name:       names.NormalizeInterfaceName("rdma-" + rdmaInfo.Name),
		Attributes: make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute),
	}

	device.Attributes[apis.AttrRDMA] = resourceapi.DeviceAttribute{BoolValue: ptr.To(true)}
	device.Attributes[apis.AttrRDMADeviceName] = resourceapi.DeviceAttribute{StringValue: ptr.To(rdmaInfo.Name)}

	if rdmaInfo.NodeGUID != "" {
		device.Attributes[apis.AttrRDMANodeGUID] = resourceapi.DeviceAttribute{StringValue: ptr.To(rdmaInfo.NodeGUID)}
	}
	if rdmaInfo.NodeType != "" {
		device.Attributes[apis.AttrRDMANodeType] = resourceapi.DeviceAttribute{StringValue: ptr.To(rdmaInfo.NodeType)}
	}
	if rdmaInfo.NumPorts > 0 {
		device.Attributes[apis.AttrRDMAPortCount] = resourceapi.DeviceAttribute{IntValue: ptr.To(int64(rdmaInfo.NumPorts))}
	}
	if rdmaInfo.PCIAddress != "" {
		device.Attributes[apis.AttrPCIAddress] = resourceapi.DeviceAttribute{StringValue: ptr.To(rdmaInfo.PCIAddress)}
	}
	if uverbsPath := GetUverbsForRDMADevice(rdmaInfo.Name); uverbsPath != "" {
		device.Attributes[apis.AttrRDMAUverbsDev] = resourceapi.DeviceAttribute{StringValue: ptr.To(uverbsPath)}
	}

	return device
}

func (db *DB) addCloudAttributes(devices []resourceapi.Device) []resourceapi.Device {
	for i := range devices {
		device := &devices[i]
		mac, ok := device.Attributes[apis.AttrMac]
		if !ok || mac.StringValue == nil {
			continue
		}
		maps.Copy(device.Attributes, getProviderAttributes(*mac.StringValue, db.instance))
	}
	return devices
}

func (db *DB) updateDeviceStore(devices []resourceapi.Device) {
	deviceStore := map[string]resourceapi.Device{}
	for _, device := range devices {
		deviceStore[device.Name] = device
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.deviceStore = deviceStore
}

func (db *DB) getDevice(deviceName string) (resourceapi.Device, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	device, exists := db.deviceStore[deviceName]
	return device, exists
}

// GetNetInterfaceName returns the network interface name and the full device
// for a given device name. It first attempts to retrieve from the local device
// store. If the device is not found, it triggers a rescan of the system's
// devices and retries. This ensures newly added devices can be found.
//
// Returns:
//   - interface name (empty string if device has no network interface, e.g., RDMA-only)
//   - the device object
//   - error if device not found even after rescan
func (db *DB) GetNetInterfaceName(deviceName string) (string, resourceapi.Device, error) {
	device, exists := db.getDevice(deviceName)
	if !exists {
		klog.V(3).Infof("Device %q not found in local store, rescanning.", deviceName)
		db.scan()
		device, exists = db.getDevice(deviceName)
		if !exists {
			return "", resourceapi.Device{}, fmt.Errorf("device %s not found in store", deviceName)
		}
	}

	ifName := ""
	if device.Attributes[apis.AttrInterfaceName].StringValue != nil {
		ifName = *device.Attributes[apis.AttrInterfaceName].StringValue
	}
	return ifName, device, nil
}

// isNetworkDevice checks the class is 0x2, defined for all types of network controllers
// https://pcisig.com/sites/default/files/files/PCI_Code-ID_r_1_11__v24_Jan_2019.pdf
func isNetworkDevice(dev *ghw.PCIDevice) bool {
	return dev.Class.ID == "02"
}

// truncateString truncates the string to the specified maximum number of bytes.
// It ensures the truncation doesn't break UTF-8 encoding by truncating at rune boundaries.
func truncateString(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	// Truncate and ensure we don't cut in the middle of a UTF-8 rune
	for maxBytes > 0 && s[maxBytes-1]&0xC0 == 0x80 {
		maxBytes--
	}
	return s[:maxBytes]
}
