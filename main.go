package main

import (
	"DistributedDB/config"
	"DistributedDB/db"
	"DistributedDB/web"
	"flag"   
 	"log"
	"net/http"
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

	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config %q: %v", *configFile, err)
	}

	shards, err := config.ParseShards(c.Shards, *shard)

	if err != nil {
		log.Fatalf("Error parsing shards config: %v", err)
	}
	log.Printf("Shard count is %d, current shard: %d\n", shards.Count, shards.CurIdx)

	db, close, err := db.NewDatabase(*dbLocation)
	defer close()

	server := web.NewServer(db, shards)
	if err != nil {
		log.Fatalf("NewDatabase(%q): %v\n", *dbLocation, err)
	}

	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)

	log.Fatal(server.ListenAndServe())
}
