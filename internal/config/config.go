// Package config loads and parses the cluster configuration file.
//
// The config file (config/cluster.json) tells each node:
//   - Who it is (its own ID)
//   - What port to listen on for Raft RPCs (raft_addr)
//   - What port to expose for client KV commands (kv_addr)
//   - Who all the other nodes are (so it can dial them for Raft)
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// NodeConfig holds the addresses for a single node in the cluster.
type NodeConfig struct {
	ID string `json:"id"`

	// RaftAddr is the TCP address this node's Raft RPC server listens on.
	RaftAddr string `json:"raft_addr"`

	// KVAddr is the TCP address clients connect to for GET/PUT/DELETE commands.
	KVAddr string `json:"kv_addr"`

	// HTTPAddr is the TCP address for the HTTP/JSON API and dashboard.
	HTTPAddr string `json:"http_addr"`
}

// ClusterConfig holds the full list of nodes in the cluster.
type ClusterConfig struct {
	Nodes []NodeConfig `json:"nodes"`
}

// Load reads and parses the JSON config file at path.
// Returns an error if the file is missing or malformed.
func Load(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg ClusterConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("config %s: no nodes defined", path)
	}

	return &cfg, nil
}

// Self returns the NodeConfig for the node with the given id.
// Returns an error if no node with that id exists in the config.
func (c *ClusterConfig) Self(id string) (*NodeConfig, error) {
	for i := range c.Nodes {
		if c.Nodes[i].ID == id {
			return &c.Nodes[i], nil
		}
	}
	return nil, fmt.Errorf("node %q not found in cluster config", id)
}

// Peers returns a map of id → raft_addr for every node EXCEPT myID.
func (c *ClusterConfig) Peers(myID string) map[string]string {
	peers := make(map[string]string, len(c.Nodes)-1)
	for _, n := range c.Nodes {
		if n.ID != myID {
			peers[n.ID] = n.RaftAddr
		}
	}
	return peers
}
