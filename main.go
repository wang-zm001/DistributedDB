package main

import (
	"DistributedDB/config"
	"DistributedDB/db"
	"DistributedDB/web"
	"flag"
	"log"
	"net/http"
	"github.com/BurntSushi/toml"
)

var (
	dbLocation = flag.String("db-location", "./mydb", "The path to the bolt database")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "001", "The name of the shard for the data")
)

func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatalf("Must provide db-location")
	}
	if *shard == "" {
		log.Fatalf("Must provide shard")
	}
}

func main() {
	parseFlags()

	var c config.Config
	if _, err := toml.DecodeFile(*configFile, &c); err != nil {
		log.Fatalf("toml.DecodeFile(%q): %v\n", *configFile, err)
	}
	var shardCount int
	var shardAddr = make(map[int]string)
	var shardIdx int = -1
	for _, s := range c.Shard {
		shardAddr[s.Idx] = s.Address
		if s.Idx+1 > shardCount {
			shardCount = s.Idx + 1
		}
		if s.Name == *shard {
			shardIdx = s.Idx
		}
	}

	if shardIdx < 0 {
		log.Fatalf("Shard %q was not found", *shard)
	}
	log.Printf("Shard count is %d, current shard: %d\n", shardCount, shardIdx)

	db, close, err := db.NewDatabase(*dbLocation)
	server := web.NewServer(db, shardIdx, shardCount, shardAddr)
	if err != nil {
		log.Fatalf("NewDatabase(%q): %v\n", *dbLocation, err)
	}
	defer close()

	http.HandleFunc("/get", server.GetHandler)

	http.HandleFunc("/set", server.SetHandler)

	log.Fatal(server.ListenAndServe())
}
