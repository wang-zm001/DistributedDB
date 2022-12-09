package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/wang-zm001/DistributedDB/config"
	"github.com/wang-zm001/DistributedDB/db"

	"github.com/wang-zm001/DistributedDB/replication"
	"github.com/wang-zm001/DistributedDB/web"
)

var (
	dbLocation = flag.String("db-location", "./mydb", "The path to the bolt database")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	shard      = flag.String("shard", "num0", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Whether or not run as a read-only replica")
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

	db, close, err := db.NewDatabase(*dbLocation, *replica)
	if err != nil {
		log.Fatalf("NewDatabase(%q): %v\n", *dbLocation, err)
	}
	defer close()

	if *replica {
		leaderAddr, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Could not find address for leader for shard %d", shards.CurIdx)
		}
		log.Printf("leaderAddr is  %s", leaderAddr)
		go replication.ClientLoop(db, leaderAddr)
	}

	server := web.NewServer(db, shards)
	
	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)
	http.HandleFunc("/purge", server.DeleteExtraKeysHandler)
	http.HandleFunc("/next-replication-key", server.GetNextKeyForReplication)
	http.HandleFunc("/delete-replication-key", server.DeleteReplicationKey)

	// log.Fatal(http.ListenAndServe(*httpAddr, nil))
	log.Fatal( server.ListenAndServe(*httpAddr))
}
