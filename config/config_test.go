package config_test

import (
	"DistributedDB/config"
	"os"
	"reflect"
	"testing"
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
	name = "000"
	idx = 0
	address = "localhost:7080"
	`
	got := GetConfig(t, contents)

	want := config.Config{
		Shards: []config.Shard{
			{
				Name:    "000",
				Idx:     0,
				Address: "localhost:7080",
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
	name = "000"
	idx = 0
	address = "localhost:7080"
	[[shards]]
	name = "001"
	idx = 1
	address = "localhost:7081"
	[[shards]]
	name = "002"
	idx = 2
	address = "localhost:7082"
	`
	c := GetConfig(t, contents)

	got, err := config.ParseShards(c.Shards, "000")
	if err != nil {
		t.Fatalf("Could not parse shards: %v", err)
	}

	want := &config.Shards{
		Addrs: map[int]string{
			0: "localhost:7080",
			1: "localhost:7081",
			2: "localhost:7082",
		},
		Count: 3,
		CurIdx: 0,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("The shards doesn't match, got: %#v, wang: %#v", got, want)
	}
}
