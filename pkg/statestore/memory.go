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
	"sync"

	"k8s.io/apimachinery/pkg/types"
)

// memoryStore implements Store using in-memory maps.
// State is lost when the process exits.
type memoryStore struct {
	mu sync.RWMutex

	podNetNs   map[string]string
	podConfigs map[types.UID]map[string][]byte
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		podNetNs:   make(map[string]string),
		podConfigs: make(map[types.UID]map[string][]byte),
	}
}

func (s *memoryStore) Open() error {
	return nil
}

func (s *memoryStore) Close() error {
	return nil
}

// Pod network namespace operations

func (s *memoryStore) SetPodNetNs(podKey string, netNsPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.podNetNs[podKey] = netNsPath
	return nil
}

func (s *memoryStore) GetPodNetNs(podKey string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, ok := s.podNetNs[podKey]
	return ns, ok
}

func (s *memoryStore) DeletePodNetNs(podKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.podNetNs, podKey)
	return nil
}

func (s *memoryStore) ListPodNetNs() (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]string, len(s.podNetNs))
	for k, v := range s.podNetNs {
		result[k] = v
	}
	return result, nil
}

// Pod config operations

func (s *memoryStore) SetPodConfig(podUID types.UID, deviceName string, config []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.podConfigs[podUID]; !ok {
		s.podConfigs[podUID] = make(map[string][]byte)
	}
	// Store a copy to prevent external modification
	configCopy := make([]byte, len(config))
	copy(configCopy, config)
	s.podConfigs[podUID][deviceName] = configCopy
	return nil
}

func (s *memoryStore) GetPodConfig(podUID types.UID, deviceName string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if podConfigs, ok := s.podConfigs[podUID]; ok {
		config, found := podConfigs[deviceName]
		if found {
			// Return a copy
			configCopy := make([]byte, len(config))
			copy(configCopy, config)
			return configCopy, true
		}
	}
	return nil, false
}

func (s *memoryStore) GetPodConfigs(podUID types.UID) (map[string][]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	podConfigs, found := s.podConfigs[podUID]
	if !found {
		return nil, false
	}
	result := make(map[string][]byte, len(podConfigs))
	for k, v := range podConfigs {
		configCopy := make([]byte, len(v))
		copy(configCopy, v)
		result[k] = configCopy
	}
	return result, true
}

func (s *memoryStore) DeletePod(podUID types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.podConfigs, podUID)
	return nil
}

func (s *memoryStore) DeletePodConfigsByClaim(claimNamespace, claimName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// We need to parse each config to check the claim
	podsToDelete := []types.UID{}
	for uid, podConfigsMap := range s.podConfigs {
		for _, configBytes := range podConfigsMap {
			var claimRef struct {
				Claim struct {
					Namespace string `json:"Namespace"`
					Name      string `json:"Name"`
				} `json:"Claim"`
			}
			if err := json.Unmarshal(configBytes, &claimRef); err != nil {
				continue
			}
			if claimRef.Claim.Namespace == claimNamespace && claimRef.Claim.Name == claimName {
				podsToDelete = append(podsToDelete, uid)
				break
			}
		}
	}

	for _, uid := range podsToDelete {
		delete(s.podConfigs, uid)
	}
	return nil
}

func (s *memoryStore) ListAllPodConfigs() (map[types.UID]map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[types.UID]map[string][]byte, len(s.podConfigs))
	for uid, podConfigs := range s.podConfigs {
		configsCopy := make(map[string][]byte, len(podConfigs))
		for k, v := range podConfigs {
			configCopy := make([]byte, len(v))
			copy(configCopy, v)
			configsCopy[k] = configCopy
		}
		result[uid] = configsCopy
	}
	return result, nil
}
