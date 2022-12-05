package main

import (
	"DistributedDB/db"
	"DistributedDB/web"
	"flag"
	"log"
	"net/http"
)

var (
	dbLocation = flag.String("db-location", "./mydb", "The path to the bolt database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
)

func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatalf("Must provide db-location")
	}
}
 
func main() {
	parseFlags()
	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist.
	db, close, err := db.NewDatabase(*dbLocation)
	server := web.NewServer(db)
	if err != nil {
		log.Fatalf("NewDatabase(%q): %v", *dbLocation, err)
	}
	defer close()

	http.HandleFunc("/get", server.GetHandler)

	http.HandleFunc("/set", server.SetHandler)

	log.Fatal(server.ListenAndServe(*httpAddr))
}
