package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/wang-zm001/DistributedDB/db"
	"github.com/wang-zm001/DistributedDB/zookeeper"

	"github.com/wang-zm001/DistributedDB/replication"
	"github.com/wang-zm001/DistributedDB/web"
)

var (
	dbLocation = flag.String("db-location", "./db/file/mydb", "The path to the bolt database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	shard      = flag.String("shard", "num0", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Whether or not run as a read-only replica")
)

var (
	zkAddress = []string{
		"127.0.0.1:2181",
	}
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

	conn, err := zookeeper.Connect(zkAddress)
	if err != nil {
		log.Fatalf("Error connect zookeeper server, %v", err)
	}
	defer conn.Close()
	
	node := &zookeeper.ZkNode {
		Path: "/" + *shard,
		Name: *shard,
		Host: *httpAddr,
	}
	isExist, err := zookeeper.IsExist(conn, node.Path)
	if err != nil {
		log.Fatalf("Error Get Node, %v", err)
	}
	if !isExist {
		if err = zookeeper.AddZkNode(node, conn); err != nil {
			log.Fatalf("Error addZkNode, %v", err)
		}
	}

	db, close, err := db.NewDatabase(*dbLocation, *replica)
	if err != nil {
		log.Fatalf("NewDatabase(%q): %v\n", *dbLocation, err)
	}
	defer close()

	if *replica {
		var master zookeeper.ZkNode 
		err := zookeeper.GetZkNode("/" + *shard, conn, &master)
		if err != nil{
			log.Fatalf("Could not find address for leader for shard %s, err: %v", *shard, err)
		}
		leaderAddr := master.Host
		log.Printf("[Replication] address is %s, leaderAddr is  %s",*httpAddr, leaderAddr)
		go replication.ClientLoop(db, leaderAddr)
	}

	server := web.NewServer(db, conn, *httpAddr, *shard)
	
	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)
	http.HandleFunc("/purge", server.DeleteExtraKeysHandler)
	http.HandleFunc("/next-replication-key", server.GetNextKeyForReplication)
	http.HandleFunc("/delete-replication-key", server.DeleteReplicationKey)

	log.Printf("[main] Server address is %s", *httpAddr)
	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}
