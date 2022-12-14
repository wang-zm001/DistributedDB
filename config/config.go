package config

import (
	"fmt"
	// "hash/fnv"

	"github.com/BurntSushi/toml"
	"github.com/wang-zm001/DistributedDB/consistenthash"
)

// Shard describes a shard that holds the appropriates set of keys
// Each shard has unique set of keys.
type Shard struct {
	Name    string
	Idx     int
	Address string
}

// Config descirbes the sharding config
type Config struct {
	Shards []Shard
}

// Shards represents an easier-to-use representation of
// the sharding config: the shards count, current index and
// the address of all other shards too.
type Shards struct {
	Count  int
	CurIdx int
	CurAddr string
	ShardMap *consistenthash.Map
	Addrs  map[int]string
}

func ParseShards(shards []Shard, curShardName string) (*Shards, error) {
	shardCount := len(shards)
	shardIdx := -1
	addrsMap := make(map[int]string)
	shardMap := consistenthash.New(3, nil)
	addrs := make([]string, shardCount)

	for index, s := range shards {
		if _, ok := addrsMap[s.Idx]; ok {
			return nil, fmt.Errorf("dulicate shard index: %d", s.Idx)
		}

		addrsMap[s.Idx] = s.Address
		addrs[index] = s.Address
		if s.Name == curShardName {
			shardIdx = s.Idx
		}
	}

	for i := 0; i < shardCount; i++ {
		if _, ok := addrsMap[i]; !ok {
			return nil, fmt.Errorf("shard %d is not found", i)
		}
	}

	if shardIdx < 0 {
		return nil, fmt.Errorf("shard %q was not found", curShardName)
	}

	shardMap.Add(addrs...)
	return &Shards{
		Addrs: addrsMap,
		Count: shardCount,
		CurAddr: addrs[shardIdx],
		CurIdx: shardIdx,
		ShardMap: shardMap,
	}, nil
}

// ParseFile parses the config and returns it upon success.
func ParseFile(filename string) (Config, error) {
	var c Config
	if _, err := toml.DecodeFile(filename, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}