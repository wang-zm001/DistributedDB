package config

// Shard describes a shard that holds the appropriates set of keys
// Each shard has unique set of keys.
type Shard struct {
	Name string
	Idx  int
	Address string
}

// Config descirbes the sharding config
type Config struct {
	Shard []Shard
}
