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

// Package statestore provides an abstraction for persisting DRAnet state.
// It supports both in-memory (default) and persistent (bbolt) backends,
// allowing state to survive restarts when using the persistent backend.
package statestore

import (
	"k8s.io/apimachinery/pkg/types"
)

// Store defines the interface for persisting DRAnet state.
// Implementations must be thread-safe.
type Store interface {
	// Open initializes the store. Must be called before any other operations.
	Open() error

	// Close releases any resources held by the store.
	Close() error

	// Pod network namespace operations
	SetPodNetNs(podKey string, netNsPath string) error
	GetPodNetNs(podKey string) (string, bool)
	DeletePodNetNs(podKey string) error
	ListPodNetNs() (map[string]string, error)

	// Pod config operations
	SetPodConfig(podUID types.UID, deviceName string, config []byte) error
	GetPodConfig(podUID types.UID, deviceName string) ([]byte, bool)
	GetPodConfigs(podUID types.UID) (map[string][]byte, bool)
	DeletePod(podUID types.UID) error
	DeletePodConfigsByClaim(claimNamespace, claimName string) error
	ListAllPodConfigs() (map[types.UID]map[string][]byte, error)
}

// Type represents the type of state store backend.
type Type string

const (
	// TypeMemory uses an in-memory store (default, state lost on restart)
	TypeMemory Type = "memory"
	// TypeBBolt uses bbolt for persistent storage
	TypeBBolt Type = "bbolt"
)

// Config holds configuration for creating a state store.
type Config struct {
	// Type specifies the backend type (memory or bbolt)
	Type Type
	// Path is the file path for persistent stores (ignored for memory)
	Path string
}

// New creates a new Store based on the provided configuration.
func New(cfg Config) Store {
	switch cfg.Type {
	case TypeBBolt:
		return newBBoltStore(cfg.Path)
	case TypeMemory:
		fallthrough
	default:
		return newMemoryStore()
	}
}
