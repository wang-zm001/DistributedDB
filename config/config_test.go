package config_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/wang-zm001/DistributedDB/config"
)

func GetConfig(t *testing.T, contents string) config.Config {
	t.Helper()

	f, err := os.CreateTemp(os.TempDir(), "config.toml")
	if err != nil {
		t.Fatalf("Create temp config.toml err: %v", err)
	}
	defer f.Close()

	name := f.Name()
	defer os.Remove(name)

	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatalf("Could not write the config contents: %v", err)
	}

	c, err := config.ParseFile(name)
	if err != nil {
		t.Fatalf("Could not parse config: %v", err)
	}

	return c
}

func TestConfigParse(t *testing.T) {
	contents := `[[shards]]
	Name = "db0"
	Idx = 0
	Address = "127.0.0.1:8080"
	Address-replica = ["127.0.0.1:8090"]
	
	[[shards]]
	Name = "db1"
	Idx = 1
	Address = "127.0.0.1:8081"
	Address-replica = ["127.0.0.1:8091"]
	`
	got := GetConfig(t, contents)

	want := config.Config{
		Shards: []config.Shard{
			{
				Name:    "db0",
				Idx:     0,
				Address: "127.0.0.1:8080",
			},
			{
				Name:    "db1",
				Idx:     1,
				Address: "127.0.0.1:8081",
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("The config doesn't match, got: %#v, wang: %#v", got, want)
	}
}

func TestParseShards(t *testing.T) {
	contents := `
	[[shards]]
	Name = "db0"
	Idx = 0
	Address = "127.0.0.1:8080"
	Address-replica = ["127.0.0.1:8090"]
	
	[[shards]]
	Name = "db1"
	Idx = 1
	Address = "127.0.0.1:8081"
	Address-replica = ["127.0.0.1:8091"]
	`
	c := GetConfig(t, contents)

	got, err := config.ParseShards(c.Shards, "db0")
	if err != nil {
		t.Fatalf("Could not parse shards: %v", err)
	}

	shardMap := got.ShardMap
	want := &config.Shards{
		Count: 2,
		CurIdx: 0,
		CurAddr: "127.0.0.1:8080",
		ShardMap: shardMap,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("The shards doesn't match, got: %#v, wang: %#v", got, want)
	}
}
