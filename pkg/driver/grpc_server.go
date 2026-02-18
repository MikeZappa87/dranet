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
	"errors"
	"fmt"
	"net"
	"syscall"

	pb "github.com/google/dranet/api/networking/v1"
	"github.com/google/dranet/internal/nlwrap"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// PodNetworkServer implements the PodNetwork gRPC service.
type PodNetworkServer struct {
	pb.UnimplementedPodNetworkServer
	driver *NetworkDriver
}

// NewPodNetworkServer creates a new PodNetworkServer.
func NewPodNetworkServer(driver *NetworkDriver) *PodNetworkServer {
	return &PodNetworkServer{driver: driver}
}

// RegisterServer registers the PodNetworkServer with a gRPC server.
func (s *PodNetworkServer) RegisterServer(grpcServer *grpc.Server) {
	pb.RegisterPodNetworkServer(grpcServer, s)
}

// getPodNetnsPath validates the sandbox ID and returns the netns path.
func (s *PodNetworkServer) getPodNetnsPath(sandboxID string) (string, error) {
	if sandboxID == "" {
		return "", status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	netnsPath := s.driver.netdb.GetPodNetNs(sandboxID)
	if netnsPath == "" {
		return "", status.Errorf(codes.NotFound, "pod sandbox %s not found", sandboxID)
	}
	return netnsPath, nil
}

// podNetnsHandle holds a netlink handle and the namespace handle that must be closed.
type podNetnsHandle struct {
	nlwrap.Handle
	ns netns.NsHandle
}

// Close closes both the netlink handle and the namespace handle.
func (h *podNetnsHandle) Close() {
	h.Handle.Close()
	h.ns.Close()
}

// getPodNetlinkHandle returns a netlink handle for the pod's network namespace.
func (s *PodNetworkServer) getPodNetlinkHandle(sandboxID string) (*podNetnsHandle, error) {
	netnsPath, err := s.getPodNetnsPath(sandboxID)
	if err != nil {
		return nil, err
	}

	containerNs, err := netns.GetFromPath(netnsPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get netns: %v", err)
	}

	nhNs, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		containerNs.Close()
		return nil, status.Errorf(codes.Internal, "failed to get netlink handle: %v", err)
	}

	return &podNetnsHandle{Handle: nhNs, ns: containerNs}, nil
}

// getNetlinkHandleFor returns a netlink handle for either the pod sandbox or host namespace.
// If hostNetwork is true, returns a handle for the host namespace.
func (s *PodNetworkServer) getNetlinkHandleFor(sandboxID string, hostNetwork bool) (*podNetnsHandle, error) {
	if hostNetwork {
		nh, err := nlwrap.NewHandle()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get netlink handle: %v", err)
		}
		return &podNetnsHandle{Handle: nh}, nil
	}
	return s.getPodNetlinkHandle(sandboxID)
}

// GetPodResources returns resources associated with a pod sandbox.
func (s *PodNetworkServer) GetPodResources(ctx context.Context, req *pb.GetPodResourcesRequest) (*pb.GetPodResourcesResponse, error) {
	netnsPath, err := s.getPodNetnsPath(req.SandboxId)
	if err != nil {
		return nil, err
	}

	return &pb.GetPodResourcesResponse{
		PodNetnsPath: netnsPath,
	}, nil
}

// GetPodIPs returns the IP addresses assigned to a pod sandbox.
func (s *PodNetworkServer) GetPodIPs(ctx context.Context, req *pb.GetPodIPsRequest) (*pb.GetPodIPsResponse, error) {
	nh, err := s.getPodNetlinkHandle(req.SandboxId)
	if err != nil {
		return nil, err
	}
	defer nh.Close()

	links, err := nh.LinkList()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list links: %v", err)
	}

	interfaceIPs := make(map[string]*pb.PodInterfaceIPs)
	for _, link := range links {
		addrs, err := nh.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			klog.Warningf("failed to list addresses for %s: %v", link.Attrs().Name, err)
			continue
		}
		ips := &pb.PodInterfaceIPs{}
		for _, addr := range addrs {
			ips.Ips = append(ips.Ips, addr.IPNet.String())
		}
		if len(ips.Ips) > 0 {
			interfaceIPs[link.Attrs().Name] = ips
		}
	}

	routes, err := nh.RouteList(nil, netlink.FAMILY_ALL)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list routes: %v", err)
	}

	var podRoutes []*pb.PodRoute
	for _, route := range routes {
		pr := &pb.PodRoute{}
		if route.Dst == nil {
			pr.Destination = "default"
		} else {
			pr.Destination = route.Dst.String()
		}
		if route.Gw != nil {
			pr.Gateway = route.Gw.String()
		}
		// Find interface name by index
		for _, link := range links {
			if link.Attrs().Index == route.LinkIndex {
				pr.InterfaceName = link.Attrs().Name
				break
			}
		}
		podRoutes = append(podRoutes, pr)
	}

	return &pb.GetPodIPsResponse{
		InterfaceIps: interfaceIPs,
		Routes:       podRoutes,
	}, nil
}

// GetPodNetwork returns the full network state of a pod sandbox.
func (s *PodNetworkServer) GetPodNetwork(ctx context.Context, req *pb.GetPodNetworkRequest) (*pb.GetPodNetworkResponse, error) {
	nh, err := s.getPodNetlinkHandle(req.SandboxId)
	if err != nil {
		return nil, err
	}
	defer nh.Close()

	links, err := nh.LinkList()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list links: %v", err)
	}

	// Build interface list
	var interfaces []*pb.NetworkInterface
	linkIndexToName := make(map[int]string)
	for _, link := range links {
		attrs := link.Attrs()
		linkIndexToName[attrs.Index] = attrs.Name

		iface := &pb.NetworkInterface{
			Name:       attrs.Name,
			MacAddress: attrs.HardwareAddr.String(),
			Type:       pb.DeviceType_NETDEV,
			Mtu:        uint32(attrs.MTU),
			State:      attrs.OperState.String(),
		}

		addrs, err := nh.AddrList(link, netlink.FAMILY_ALL)
		if err == nil {
			for _, addr := range addrs {
				iface.Addresses = append(iface.Addresses, addr.IPNet.String())
			}
		}
		interfaces = append(interfaces, iface)
	}

	// Get RDMA devices
	rdmaLinks, err := nh.RdmaLinkList()
	if err == nil {
		for _, rdma := range rdmaLinks {
			iface := &pb.NetworkInterface{
				Name: rdma.Attrs.Name,
				Type: pb.DeviceType_RDMA,
			}
			interfaces = append(interfaces, iface)
		}
	}

	// Build route list
	routes, err := nh.RouteList(nil, netlink.FAMILY_ALL)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list routes: %v", err)
	}

	var routeEntries []*pb.RouteEntry
	for _, route := range routes {
		entry := &pb.RouteEntry{
			InterfaceName: linkIndexToName[route.LinkIndex],
			Metric:        uint32(route.Priority),
			Scope:         scopeToString(route.Scope),
		}
		if route.Dst == nil {
			entry.Destination = "default"
		} else {
			entry.Destination = route.Dst.String()
		}
		if route.Gw != nil {
			entry.Gateway = route.Gw.String()
		}
		routeEntries = append(routeEntries, entry)
	}

	// Build rule list
	rules, err := nh.RuleList(netlink.FAMILY_ALL)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list rules: %v", err)
	}

	var ruleEntries []*pb.RoutingRule
	for _, rule := range rules {
		entry := &pb.RoutingRule{
			Priority: uint32(rule.Priority),
			Table:    fmt.Sprintf("%d", rule.Table),
		}
		if rule.Src != nil {
			entry.Src = rule.Src.String()
		}
		if rule.Dst != nil {
			entry.Dst = rule.Dst.String()
		}
		if rule.IifName != "" {
			entry.Iif = rule.IifName
		}
		if rule.OifName != "" {
			entry.Oif = rule.OifName
		}
		ruleEntries = append(ruleEntries, entry)
	}

	return &pb.GetPodNetworkResponse{
		Interfaces: interfaces,
		Routes:     routeEntries,
		Rules:      ruleEntries,
	}, nil
}

// MoveDevice moves a network device into a pod sandbox's network namespace.
func (s *PodNetworkServer) MoveDevice(ctx context.Context, req *pb.MoveDeviceRequest) (*pb.MoveDeviceResponse, error) {
	if req.DeviceName == "" {
		return nil, status.Error(codes.InvalidArgument, "device_name is required")
	}

	netnsPath, err := s.getPodNetnsPath(req.SandboxId)
	if err != nil {
		return nil, err
	}

	var finalName string
	var addresses []string
	var routeEntries []*pb.RouteEntry
	var ruleEntries []*pb.RoutingRule

	if req.DeviceType == pb.DeviceType_RDMA {
		// Move RDMA device
		if err := nsAttachRdmadev(req.DeviceName, netnsPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to move RDMA device: %v", err)
		}
		finalName = req.DeviceName
	} else {
		// Get device info before moving
		hostDev, err := nlwrap.LinkByName(req.DeviceName)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "device %s not found: %v", req.DeviceName, err)
		}

		// Capture addresses before moving
		addrs, err := netlink.AddrList(hostDev, netlink.FAMILY_ALL)
		if err == nil {
			for _, addr := range addrs {
				addresses = append(addresses, addr.IPNet.String())
			}
		}

		// Capture routes before moving
		routes, err := netlink.RouteList(hostDev, netlink.FAMILY_ALL)
		if err == nil {
			for _, route := range routes {
				entry := &pb.RouteEntry{
					InterfaceName: req.DeviceName,
					Metric:        uint32(route.Priority),
					Scope:         scopeToString(route.Scope),
				}
				if route.Dst == nil {
					entry.Destination = "default"
				} else {
					entry.Destination = route.Dst.String()
				}
				if route.Gw != nil {
					entry.Gateway = route.Gw.String()
				}
				routeEntries = append(routeEntries, entry)
			}
		}

		// Move netdev
		targetName := req.TargetName
		if targetName == "" {
			targetName = req.DeviceName
		}

		containerNs, err := netns.GetFromPath(netnsPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get netns: %v", err)
		}
		defer containerNs.Close()

		// Set device down before moving
		if err := netlink.LinkSetDown(hostDev); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to set device down: %v", err)
		}

		// Move to namespace
		if err := netlink.LinkSetNsFd(hostDev, int(containerNs)); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to move device to namespace: %v", err)
		}

		// Configure in new namespace
		nhNs, err := nlwrap.NewHandleAt(containerNs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get netlink handle: %v", err)
		}
		defer nhNs.Close()

		nsLink, err := nhNs.LinkByName(req.DeviceName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "device not found in namespace: %v", err)
		}

		// Rename if needed
		if targetName != req.DeviceName {
			if err := nhNs.LinkSetName(nsLink, targetName); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to rename device: %v", err)
			}
			nsLink, _ = nhNs.LinkByName(targetName)
		}

		// Restore addresses
		for _, addrStr := range addresses {
			ip, ipnet, err := net.ParseCIDR(addrStr)
			if err != nil {
				continue
			}
			if err := nhNs.AddrAdd(nsLink, &netlink.Addr{IPNet: &net.IPNet{IP: ip, Mask: ipnet.Mask}}); err != nil && !errors.Is(err, syscall.EEXIST) {
				klog.Warningf("failed to restore address %s: %v", addrStr, err)
			}
		}

		// Set device up
		if err := nhNs.LinkSetUp(nsLink); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to set device up: %v", err)
		}

		finalName = targetName
	}

	return &pb.MoveDeviceResponse{
		DeviceName: finalName,
		Addresses:  addresses,
		Routes:     routeEntries,
		Rules:      ruleEntries,
	}, nil
}

// AssignIPAddress assigns an IP address to an interface within the pod sandbox.
func (s *PodNetworkServer) AssignIPAddress(ctx context.Context, req *pb.AssignIPAddressRequest) (*pb.AssignIPAddressResponse, error) {
	if req.InterfaceName == "" {
		return nil, status.Error(codes.InvalidArgument, "interface_name is required")
	}
	if req.Address == "" {
		return nil, status.Error(codes.InvalidArgument, "address is required")
	}

	nh, err := s.getPodNetlinkHandle(req.SandboxId)
	if err != nil {
		return nil, err
	}
	defer nh.Close()

	nsLink, err := nh.LinkByName(req.InterfaceName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "interface %s not found: %v", req.InterfaceName, err)
	}

	ip, ipnet, err := net.ParseCIDR(req.Address)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid address: %v", err)
	}

	if err := nh.AddrAdd(nsLink, &netlink.Addr{IPNet: &net.IPNet{IP: ip, Mask: ipnet.Mask}}); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, status.Errorf(codes.Internal, "failed to add address: %v", err)
	}

	return &pb.AssignIPAddressResponse{}, nil
}

// ApplyRoute adds a route in the pod sandbox's network namespace.
func (s *PodNetworkServer) ApplyRoute(ctx context.Context, req *pb.ApplyRouteRequest) (*pb.ApplyRouteResponse, error) {
	if req.SandboxId == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	if req.Route == nil {
		return nil, status.Error(codes.InvalidArgument, "route is required")
	}

	nh, err := s.getNetlinkHandleFor(req.SandboxId, req.HostNetwork)
	if err != nil {
		return nil, err
	}
	defer nh.Close()

	route := &netlink.Route{
		Scope: stringToScope(req.Route.Scope),
	}

	if req.Route.InterfaceName != "" {
		link, err := nh.LinkByName(req.Route.InterfaceName)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "interface %s not found: %v", req.Route.InterfaceName, err)
		}
		route.LinkIndex = link.Attrs().Index
	}

	if req.Route.Destination != "" && req.Route.Destination != "default" {
		_, dst, err := net.ParseCIDR(req.Route.Destination)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid destination: %v", err)
		}
		route.Dst = dst
	}

	if req.Route.Gateway != "" {
		route.Gw = net.ParseIP(req.Route.Gateway)
		if route.Gw == nil {
			return nil, status.Error(codes.InvalidArgument, "invalid gateway address")
		}
	}

	route.Priority = int(req.Route.Metric)

	if err := nh.RouteAdd(route); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, status.Errorf(codes.Internal, "failed to add route: %v", err)
	}

	return &pb.ApplyRouteResponse{}, nil
}

// ApplyRule adds an ip rule in the pod sandbox's network namespace.
func (s *PodNetworkServer) ApplyRule(ctx context.Context, req *pb.ApplyRuleRequest) (*pb.ApplyRuleResponse, error) {
	if req.SandboxId == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	if req.Rule == nil {
		return nil, status.Error(codes.InvalidArgument, "rule is required")
	}

	nh, err := s.getNetlinkHandleFor(req.SandboxId, req.HostNetwork)
	if err != nil {
		return nil, err
	}
	defer nh.Close()

	rule := netlink.NewRule()
	rule.Priority = int(req.Rule.Priority)

	if req.Rule.Src != "" {
		_, src, err := net.ParseCIDR(req.Rule.Src)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid source: %v", err)
		}
		rule.Src = src
	}

	if req.Rule.Dst != "" {
		_, dst, err := net.ParseCIDR(req.Rule.Dst)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid destination: %v", err)
		}
		rule.Dst = dst
	}

	if req.Rule.Table != "" {
		var table int
		if _, err := fmt.Sscanf(req.Rule.Table, "%d", &table); err == nil {
			rule.Table = table
		} else {
			// Handle named tables (main, local, default)
			switch req.Rule.Table {
			case "main":
				rule.Table = 254
			case "local":
				rule.Table = 255
			case "default":
				rule.Table = 253
			default:
				return nil, status.Errorf(codes.InvalidArgument, "invalid table: %s", req.Rule.Table)
			}
		}
	}

	if req.Rule.Iif != "" {
		rule.IifName = req.Rule.Iif
	}
	if req.Rule.Oif != "" {
		rule.OifName = req.Rule.Oif
	}

	if err := nh.RuleAdd(rule); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, status.Errorf(codes.Internal, "failed to add rule: %v", err)
	}

	return &pb.ApplyRuleResponse{}, nil
}

// CreateNetdev creates a new network device inside the pod sandbox.
func (s *PodNetworkServer) CreateNetdev(ctx context.Context, req *pb.CreateNetdevRequest) (*pb.CreateNetdevResponse, error) {
	if req.SandboxId == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	// Get handle for target namespace (pod or host)
	nh, err := s.getNetlinkHandleFor(req.SandboxId, req.HostNetwork)
	if err != nil {
		return nil, err
	}
	defer nh.Close()

	var link netlink.Link
	var peerLink netlink.Link

	switch config := req.Config.(type) {
	case *pb.CreateNetdevRequest_Veth:
		// Veth creation is special: we create in host namespace, then move one end to pod
		if !req.HostNetwork {
			// Need the pod namespace handle for moving the veth end
			podNh, err := s.getPodNetlinkHandle(req.SandboxId)
			if err != nil {
				return nil, err
			}
			defer podNh.Close()

			veth := &netlink.Veth{
				LinkAttrs: netlink.LinkAttrs{
					Name:    req.Name,
					NetNsID: int(podNh.ns),
				},
				PeerName: config.Veth.PeerName,
			}
			if req.Mtu > 0 {
				veth.LinkAttrs.MTU = int(req.Mtu)
			}

			// Create veth in host namespace, one end goes to container
			if err := netlink.LinkAdd(veth); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to create veth: %v", err)
			}

			// Get the link in the container namespace
			link, err = podNh.LinkByName(req.Name)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get veth in namespace: %v", err)
			}

			// Get peer link info (in host namespace)
			peerLink, _ = netlink.LinkByName(config.Veth.PeerName)

			// Handle peer_master - attach peer to a bridge in host namespace
			if config.Veth.PeerMaster != "" && peerLink != nil {
				bridge, err := netlink.LinkByName(config.Veth.PeerMaster)
				if err != nil {
					return nil, status.Errorf(codes.NotFound, "bridge %s not found: %v", config.Veth.PeerMaster, err)
				}
				if err := netlink.LinkSetMaster(peerLink, bridge); err != nil {
					return nil, status.Errorf(codes.Internal, "failed to attach peer to bridge: %v", err)
				}
				// Ensure peer is up
				if err := netlink.LinkSetUp(peerLink); err != nil {
					klog.Warningf("failed to set peer link up: %v", err)
				}
				// Refresh peer link info after attaching to bridge
				peerLink, _ = netlink.LinkByName(config.Veth.PeerName)
			}

			// Assign addresses and set link up in pod namespace
			for _, addrStr := range req.Addresses {
				ip, ipnet, err := net.ParseCIDR(addrStr)
				if err != nil {
					klog.Warningf("invalid address %s: %v", addrStr, err)
					continue
				}
				if err := podNh.AddrAdd(link, &netlink.Addr{IPNet: &net.IPNet{IP: ip, Mask: ipnet.Mask}}); err != nil && !errors.Is(err, syscall.EEXIST) {
					klog.Warningf("failed to add address %s: %v", addrStr, err)
				}
			}
			if err := podNh.LinkSetUp(link); err != nil {
				klog.Warningf("failed to set link up: %v", err)
			}

			// Build response and return early (veth in pod namespace has special handling)
			resp := &pb.CreateNetdevResponse{
				Interface: linkToNetworkInterface(link),
			}
			if peerLink != nil {
				resp.PeerInterface = linkToNetworkInterface(peerLink)
			}
			return resp, nil
		}

		// host_network=true: create veth entirely in host namespace
		veth := &netlink.Veth{
			LinkAttrs: netlink.LinkAttrs{
				Name: req.Name,
			},
			PeerName: config.Veth.PeerName,
		}
		if req.Mtu > 0 {
			veth.LinkAttrs.MTU = int(req.Mtu)
		}

		if err := nh.LinkAdd(veth); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create veth: %v", err)
		}

		link, err = nh.LinkByName(req.Name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get veth: %v", err)
		}
		peerLink, _ = nh.LinkByName(config.Veth.PeerName)

		// Handle peer_master for host-network veth
		if config.Veth.PeerMaster != "" && peerLink != nil {
			bridge, err := nh.LinkByName(config.Veth.PeerMaster)
			if err != nil {
				return nil, status.Errorf(codes.NotFound, "bridge %s not found: %v", config.Veth.PeerMaster, err)
			}
			if err := nh.LinkSetMaster(peerLink, bridge); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to attach peer to bridge: %v", err)
			}
			if err := nh.LinkSetUp(peerLink); err != nil {
				klog.Warningf("failed to set peer link up: %v", err)
			}
			peerLink, _ = nh.LinkByName(config.Veth.PeerName)
		}

	case *pb.CreateNetdevRequest_Bridge:
		bridge := &netlink.Bridge{
			LinkAttrs: netlink.LinkAttrs{
				Name: req.Name,
			},
		}
		if req.Mtu > 0 {
			bridge.LinkAttrs.MTU = int(req.Mtu)
		}

		if err := nh.LinkAdd(bridge); err != nil && !errors.Is(err, syscall.EEXIST) {
			return nil, status.Errorf(codes.Internal, "failed to create bridge: %v", err)
		}

		link, err = nh.LinkByName(req.Name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get bridge: %v", err)
		}

		// Note: Bridge-specific settings like VlanFiltering and STP can be configured
		// via sysfs (/sys/class/net/<bridge>/bridge/vlan_filtering) if needed

	case *pb.CreateNetdevRequest_Vxlan:
		vxlan := &netlink.Vxlan{
			LinkAttrs: netlink.LinkAttrs{
				Name: req.Name,
			},
			VxlanId:  int(config.Vxlan.Vni),
			Port:     int(config.Vxlan.Port),
			Learning: config.Vxlan.Learning,
		}
		if req.Mtu > 0 {
			vxlan.LinkAttrs.MTU = int(req.Mtu)
		}
		if config.Vxlan.Group != "" {
			vxlan.Group = net.ParseIP(config.Vxlan.Group)
		}
		if config.Vxlan.Local != "" {
			vxlan.SrcAddr = net.ParseIP(config.Vxlan.Local)
		}
		if config.Vxlan.Ttl > 0 {
			vxlan.TTL = int(config.Vxlan.Ttl)
		}
		if config.Vxlan.UnderlayDevice != "" {
			parentLink, err := nh.LinkByName(config.Vxlan.UnderlayDevice)
			if err != nil {
				return nil, status.Errorf(codes.NotFound, "underlay device %s not found: %v", config.Vxlan.UnderlayDevice, err)
			}
			vxlan.VtepDevIndex = parentLink.Attrs().Index
		}

		if err := nh.LinkAdd(vxlan); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create vxlan: %v", err)
		}
		link, _ = nh.LinkByName(req.Name)

	case *pb.CreateNetdevRequest_Dummy:
		dummy := &netlink.Dummy{
			LinkAttrs: netlink.LinkAttrs{
				Name: req.Name,
			},
		}
		if req.Mtu > 0 {
			dummy.LinkAttrs.MTU = int(req.Mtu)
		}

		if err := nh.LinkAdd(dummy); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create dummy: %v", err)
		}
		link, _ = nh.LinkByName(req.Name)

	case *pb.CreateNetdevRequest_Ipvlan:
		parentLink, err := nh.LinkByName(config.Ipvlan.Parent)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "parent interface %s not found: %v", config.Ipvlan.Parent, err)
		}

		mode := netlink.IPVLAN_MODE_L2
		switch config.Ipvlan.Mode {
		case pb.IpvlanMode_IPVLAN_L3:
			mode = netlink.IPVLAN_MODE_L3
		case pb.IpvlanMode_IPVLAN_L3S:
			mode = netlink.IPVLAN_MODE_L3S
		}

		ipvlan := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        req.Name,
				ParentIndex: parentLink.Attrs().Index,
			},
			Mode: mode,
		}
		if req.Mtu > 0 {
			ipvlan.LinkAttrs.MTU = int(req.Mtu)
		}

		if err := nh.LinkAdd(ipvlan); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create ipvlan: %v", err)
		}
		link, _ = nh.LinkByName(req.Name)

	case *pb.CreateNetdevRequest_Macvlan:
		parentLink, err := nh.LinkByName(config.Macvlan.Parent)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "parent interface %s not found: %v", config.Macvlan.Parent, err)
		}

		mode := netlink.MACVLAN_MODE_BRIDGE
		switch config.Macvlan.Mode {
		case pb.MacvlanMode_MACVLAN_VEPA:
			mode = netlink.MACVLAN_MODE_VEPA
		case pb.MacvlanMode_MACVLAN_PRIVATE:
			mode = netlink.MACVLAN_MODE_PRIVATE
		case pb.MacvlanMode_MACVLAN_PASSTHRU:
			mode = netlink.MACVLAN_MODE_PASSTHRU
		case pb.MacvlanMode_MACVLAN_SOURCE:
			mode = netlink.MACVLAN_MODE_SOURCE
		}

		macvlan := &netlink.Macvlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        req.Name,
				ParentIndex: parentLink.Attrs().Index,
			},
			Mode: mode,
		}
		if req.Mtu > 0 {
			macvlan.LinkAttrs.MTU = int(req.Mtu)
		}
		if config.Macvlan.MacAddress != "" {
			mac, err := net.ParseMAC(config.Macvlan.MacAddress)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid MAC address: %v", err)
			}
			macvlan.LinkAttrs.HardwareAddr = mac
		}

		if err := nh.LinkAdd(macvlan); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create macvlan: %v", err)
		}
		link, _ = nh.LinkByName(req.Name)

	default:
		return nil, status.Error(codes.InvalidArgument, "device type configuration is required")
	}

	// Attach to master (bridge) if specified
	if req.Master != "" && link != nil {
		master, err := nh.LinkByName(req.Master)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "master device %s not found: %v", req.Master, err)
		}
		if err := nh.LinkSetMaster(link, master); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to attach to master: %v", err)
		}
	}

	// Assign addresses if provided
	for _, addrStr := range req.Addresses {
		ip, ipnet, err := net.ParseCIDR(addrStr)
		if err != nil {
			klog.Warningf("invalid address %s: %v", addrStr, err)
			continue
		}
		if err := nh.AddrAdd(link, &netlink.Addr{IPNet: &net.IPNet{IP: ip, Mask: ipnet.Mask}}); err != nil && !errors.Is(err, syscall.EEXIST) {
			klog.Warningf("failed to add address %s: %v", addrStr, err)
		}
	}

	// Set link up
	if err := nh.LinkSetUp(link); err != nil {
		klog.Warningf("failed to set link up: %v", err)
	}

	// Build response
	resp := &pb.CreateNetdevResponse{
		Interface: linkToNetworkInterface(link),
	}
	if peerLink != nil {
		resp.PeerInterface = linkToNetworkInterface(peerLink)
	}

	return resp, nil
}

// Helper functions

func scopeToString(scope netlink.Scope) string {
	switch scope {
	case netlink.SCOPE_UNIVERSE:
		return "global"
	case netlink.SCOPE_SITE:
		return "site"
	case netlink.SCOPE_LINK:
		return "link"
	case netlink.SCOPE_HOST:
		return "host"
	case netlink.SCOPE_NOWHERE:
		return "nowhere"
	default:
		return fmt.Sprintf("%d", scope)
	}
}

func stringToScope(s string) netlink.Scope {
	switch s {
	case "global", "universe":
		return netlink.SCOPE_UNIVERSE
	case "site":
		return netlink.SCOPE_SITE
	case "link":
		return netlink.SCOPE_LINK
	case "host":
		return netlink.SCOPE_HOST
	case "nowhere":
		return netlink.SCOPE_NOWHERE
	default:
		return netlink.SCOPE_UNIVERSE
	}
}

func linkToNetworkInterface(link netlink.Link) *pb.NetworkInterface {
	if link == nil {
		return nil
	}
	attrs := link.Attrs()
	return &pb.NetworkInterface{
		Name:       attrs.Name,
		MacAddress: attrs.HardwareAddr.String(),
		Type:       pb.DeviceType_NETDEV,
		Mtu:        uint32(attrs.MTU),
		State:      attrs.OperState.String(),
	}
}
