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

// Package statestore provides CLI commands for managing the DRAnet bbolt state store.
package statestore

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/google/dranet/pkg/statestore"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"
)

var (
	dbPath string
)

// StateStoreCmd is the root command for state store operations
var StateStoreCmd = &cobra.Command{
	Use:   "statestore",
	Short: "Manage the DRAnet bbolt state store",
	Long:  `Commands for viewing and managing the persistent state stored in the bbolt database.`,
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all entries in the state store",
	Long:  `List all pod network namespaces and pod configurations stored in the database.`,
	RunE:  runList,
}

var getPodNetNsCmd = &cobra.Command{
	Use:   "get-podnetns [pod-key]",
	Short: "Get a pod's network namespace path",
	Long:  `Get the network namespace path for a specific pod. Pod key format: namespace/name`,
	Args:  cobra.ExactArgs(1),
	RunE:  runGetPodNetNs,
}

var getPodConfigCmd = &cobra.Command{
	Use:   "get-podconfig [pod-uid] [device-name]",
	Short: "Get a pod's device configuration",
	Long:  `Get the device configuration for a specific pod and device.`,
	Args:  cobra.RangeArgs(1, 2),
	RunE:  runGetPodConfig,
}

var deletePodNetNsCmd = &cobra.Command{
	Use:   "delete-podnetns [pod-key]",
	Short: "Delete a pod's network namespace entry",
	Long:  `Delete the network namespace entry for a specific pod. Pod key format: namespace/name`,
	Args:  cobra.ExactArgs(1),
	RunE:  runDeletePodNetNs,
}

var deletePodConfigCmd = &cobra.Command{
	Use:   "delete-podconfig [pod-uid]",
	Short: "Delete all configurations for a pod",
	Long:  `Delete all device configurations for a specific pod UID.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runDeletePodConfig,
}

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show statistics about the state store",
	Long:  `Display statistics about the number of entries in each bucket.`,
	RunE:  runStats,
}

func init() {
	StateStoreCmd.PersistentFlags().StringVar(&dbPath, "db", "/var/lib/dranet/state.db", "Path to the bbolt database file")

	StateStoreCmd.AddCommand(listCmd)
	StateStoreCmd.AddCommand(getPodNetNsCmd)
	StateStoreCmd.AddCommand(getPodConfigCmd)
	StateStoreCmd.AddCommand(deletePodNetNsCmd)
	StateStoreCmd.AddCommand(deletePodConfigCmd)
	StateStoreCmd.AddCommand(statsCmd)
}

func openStore() (statestore.Store, error) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file does not exist: %s", dbPath)
	}

	store := statestore.New(statestore.Config{
		Type: statestore.TypeBBolt,
		Path: dbPath,
	})

	if err := store.Open(); err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return store, nil
}

func runList(cmd *cobra.Command, args []string) error {
	store, err := openStore()
	if err != nil {
		return err
	}
	defer store.Close()

	// List pod network namespaces
	podNetNs, err := store.ListPodNetNs()
	if err != nil {
		return fmt.Errorf("failed to list pod network namespaces: %w", err)
	}

	fmt.Println("=== Pod Network Namespaces ===")
	if len(podNetNs) == 0 {
		fmt.Println("  (none)")
	} else {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  POD KEY\tNETNS PATH")
		for key, path := range podNetNs {
			fmt.Fprintf(w, "  %s\t%s\n", key, path)
		}
		w.Flush()
	}

	fmt.Println()

	// List pod configurations
	podConfigs, err := store.ListAllPodConfigs()
	if err != nil {
		return fmt.Errorf("failed to list pod configs: %w", err)
	}

	fmt.Println("=== Pod Device Configurations ===")
	if len(podConfigs) == 0 {
		fmt.Println("  (none)")
	} else {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  POD UID\tDEVICE NAME\tCONFIG SIZE")
		for podUID, devices := range podConfigs {
			for deviceName, config := range devices {
				fmt.Fprintf(w, "  %s\t%s\t%d bytes\n", podUID, deviceName, len(config))
			}
		}
		w.Flush()
	}

	return nil
}

func runGetPodNetNs(cmd *cobra.Command, args []string) error {
	store, err := openStore()
	if err != nil {
		return err
	}
	defer store.Close()

	podKey := args[0]
	netNsPath, found := store.GetPodNetNs(podKey)
	if !found {
		return fmt.Errorf("pod network namespace not found for key: %s", podKey)
	}

	fmt.Printf("Pod: %s\n", podKey)
	fmt.Printf("Network Namespace: %s\n", netNsPath)
	return nil
}

func runGetPodConfig(cmd *cobra.Command, args []string) error {
	store, err := openStore()
	if err != nil {
		return err
	}
	defer store.Close()

	podUID := types.UID(args[0])

	if len(args) == 2 {
		// Get specific device config
		deviceName := args[1]
		config, found := store.GetPodConfig(podUID, deviceName)
		if !found {
			return fmt.Errorf("config not found for pod %s, device %s", podUID, deviceName)
		}

		fmt.Printf("Pod UID: %s\n", podUID)
		fmt.Printf("Device: %s\n", deviceName)
		fmt.Println("Configuration:")
		printPrettyJSON(config)
	} else {
		// Get all device configs for the pod
		configs, found := store.GetPodConfigs(podUID)
		if !found {
			return fmt.Errorf("no configs found for pod %s", podUID)
		}

		fmt.Printf("Pod UID: %s\n", podUID)
		fmt.Printf("Devices: %d\n\n", len(configs))
		for deviceName, config := range configs {
			fmt.Printf("--- Device: %s ---\n", deviceName)
			printPrettyJSON(config)
			fmt.Println()
		}
	}

	return nil
}

func runDeletePodNetNs(cmd *cobra.Command, args []string) error {
	store, err := openStore()
	if err != nil {
		return err
	}
	defer store.Close()

	podKey := args[0]

	// Check if it exists first
	_, found := store.GetPodNetNs(podKey)
	if !found {
		return fmt.Errorf("pod network namespace not found for key: %s", podKey)
	}

	if err := store.DeletePodNetNs(podKey); err != nil {
		return fmt.Errorf("failed to delete pod network namespace: %w", err)
	}

	fmt.Printf("Deleted pod network namespace entry for: %s\n", podKey)
	return nil
}

func runDeletePodConfig(cmd *cobra.Command, args []string) error {
	store, err := openStore()
	if err != nil {
		return err
	}
	defer store.Close()

	podUID := types.UID(args[0])

	// Check if it exists first
	_, found := store.GetPodConfigs(podUID)
	if !found {
		return fmt.Errorf("no configs found for pod %s", podUID)
	}

	if err := store.DeletePod(podUID); err != nil {
		return fmt.Errorf("failed to delete pod configs: %w", err)
	}

	fmt.Printf("Deleted all configurations for pod: %s\n", podUID)
	return nil
}

func runStats(cmd *cobra.Command, args []string) error {
	store, err := openStore()
	if err != nil {
		return err
	}
	defer store.Close()

	podNetNs, err := store.ListPodNetNs()
	if err != nil {
		return fmt.Errorf("failed to list pod network namespaces: %w", err)
	}

	podConfigs, err := store.ListAllPodConfigs()
	if err != nil {
		return fmt.Errorf("failed to list pod configs: %w", err)
	}

	totalDevices := 0
	for _, devices := range podConfigs {
		totalDevices += len(devices)
	}

	fmt.Println("=== State Store Statistics ===")
	fmt.Printf("Database path: %s\n", dbPath)
	fmt.Printf("Pod network namespaces: %d\n", len(podNetNs))
	fmt.Printf("Pods with configs: %d\n", len(podConfigs))
	fmt.Printf("Total device configs: %d\n", totalDevices)

	return nil
}

func printPrettyJSON(data []byte) {
	var parsed interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		// If it's not valid JSON, print raw
		fmt.Println(string(data))
		return
	}

	pretty, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		fmt.Println(string(data))
		return
	}
	fmt.Println(string(pretty))
}
