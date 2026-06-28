// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// NetworkDBDefault is the default bbolt file caching known public networks.
const NetworkDBDefault = "./networks.db"

var networksBucket = []byte("networks")

// Store is a bbolt-backed cache of known public NetworkCards.
type Store struct {
	db *bolt.DB
}

// OpenStore opens (or creates) the bbolt database at path.
func OpenStore(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open network store %q: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(networksBucket)
		return e
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init network store: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Get returns the card with the given ID, if present.
func (s *Store) Get(id string) (NetworkCard, bool, error) {
	var card NetworkCard
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(networksBucket).Get([]byte(id))
		if v == nil {
			return nil
		}
		found = true
		return json.Unmarshal(v, &card)
	})
	return card, found, err
}

// Put stores (or overwrites) a card keyed by its ID.
func (s *Store) Put(card NetworkCard) error {
	v, err := json.Marshal(card)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(networksBucket).Put([]byte(card.ID), v)
	})
}

// List returns all cached cards.
func (s *Store) List() ([]NetworkCard, error) {
	var out []NetworkCard
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(networksBucket).ForEach(func(_, v []byte) error {
			var card NetworkCard
			if e := json.Unmarshal(v, &card); e != nil {
				return e
			}
			out = append(out, card)
			return nil
		})
	})
	if out == nil {
		out = []NetworkCard{}
	}
	return out, err
}
