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
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

const (
	// Bucket names
	bucketPodNetNs   = "podNetNs"
	bucketPodConfigs = "podConfigs"
)

// bboltStore implements Store using bbolt for persistent storage.
type bboltStore struct {
	path string
	db   *bolt.DB
}

func newBBoltStore(path string) *bboltStore {
	return &bboltStore{
		path: path,
	}
}

func (s *bboltStore) Open() error {
	// Ensure directory exists
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("failed to create state directory %s: %w", dir, err)
	}

	db, err := bolt.Open(s.path, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to open bbolt database at %s: %w", s.path, err)
	}
	s.db = db

	// Create buckets if they don't exist
	err = s.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketPodNetNs)); err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", bucketPodNetNs, err)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketPodConfigs)); err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", bucketPodConfigs, err)
		}
		return nil
	})
	if err != nil {
		s.db.Close()
		return err
	}

	klog.Infof("Opened persistent state store at %s", s.path)
	return nil
}

func (s *bboltStore) Close() error {
	if s.db != nil {
		klog.Info("Closing persistent state store")
		return s.db.Close()
	}
	return nil
}

// Pod network namespace operations

func (s *bboltStore) SetPodNetNs(podKey string, netNsPath string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodNetNs))
		return b.Put([]byte(podKey), []byte(netNsPath))
	})
}

func (s *bboltStore) GetPodNetNs(podKey string) (string, bool) {
	var result string
	var found bool
	_ = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodNetNs))
		v := b.Get([]byte(podKey))
		if v != nil {
			result = string(v)
			found = true
		}
		return nil
	})
	return result, found
}

func (s *bboltStore) DeletePodNetNs(podKey string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodNetNs))
		return b.Delete([]byte(podKey))
	})
}

func (s *bboltStore) ListPodNetNs() (map[string]string, error) {
	result := make(map[string]string)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodNetNs))
		return b.ForEach(func(k, v []byte) error {
			result[string(k)] = string(v)
			return nil
		})
	})
	return result, err
}

// Pod config operations
// Keys are stored as "podUID/deviceName" in the podConfigs bucket

func podConfigKey(podUID types.UID, deviceName string) []byte {
	return []byte(string(podUID) + "/" + deviceName)
}

func (s *bboltStore) SetPodConfig(podUID types.UID, deviceName string, config []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodConfigs))
		return b.Put(podConfigKey(podUID, deviceName), config)
	})
}

func (s *bboltStore) GetPodConfig(podUID types.UID, deviceName string) ([]byte, bool) {
	var result []byte
	var found bool
	_ = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodConfigs))
		v := b.Get(podConfigKey(podUID, deviceName))
		if v != nil {
			result = make([]byte, len(v))
			copy(result, v)
			found = true
		}
		return nil
	})
	return result, found
}

func (s *bboltStore) GetPodConfigs(podUID types.UID) (map[string][]byte, bool) {
	result := make(map[string][]byte)
	prefix := []byte(string(podUID) + "/")

	_ = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodConfigs))
		c := b.Cursor()

		for k, v := c.Seek(prefix); k != nil && len(k) > len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			deviceName := string(k[len(prefix):])
			configCopy := make([]byte, len(v))
			copy(configCopy, v)
			result[deviceName] = configCopy
		}
		return nil
	})

	if len(result) == 0 {
		return nil, false
	}
	return result, true
}

func (s *bboltStore) DeletePod(podUID types.UID) error {
	prefix := []byte(string(podUID) + "/")

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodConfigs))
		c := b.Cursor()

		// Collect keys to delete (can't delete during iteration)
		var keysToDelete [][]byte
		for k, _ := c.Seek(prefix); k != nil && len(k) > len(prefix) && string(k[:len(prefix)]) == string(prefix); k, _ = c.Next() {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keysToDelete = append(keysToDelete, keyCopy)
		}

		for _, key := range keysToDelete {
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *bboltStore) DeletePodConfigsByClaim(claimNamespace, claimName string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodConfigs))
		c := b.Cursor()

		var keysToDelete [][]byte
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var claimRef struct {
				Claim struct {
					Namespace string `json:"Namespace"`
					Name      string `json:"Name"`
				} `json:"Claim"`
			}
			if err := json.Unmarshal(v, &claimRef); err != nil {
				continue
			}
			if claimRef.Claim.Namespace == claimNamespace && claimRef.Claim.Name == claimName {
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				keysToDelete = append(keysToDelete, keyCopy)
			}
		}

		for _, key := range keysToDelete {
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *bboltStore) ListAllPodConfigs() (map[types.UID]map[string][]byte, error) {
	result := make(map[types.UID]map[string][]byte)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPodConfigs))
		return b.ForEach(func(k, v []byte) error {
			// Parse key: "podUID/deviceName"
			key := string(k)
			for i := 0; i < len(key); i++ {
				if key[i] == '/' {
					podUID := types.UID(key[:i])
					deviceName := key[i+1:]

					if _, ok := result[podUID]; !ok {
						result[podUID] = make(map[string][]byte)
					}
					configCopy := make([]byte, len(v))
					copy(configCopy, v)
					result[podUID][deviceName] = configCopy
					break
				}
			}
			return nil
		})
	})
	return result, err
}
