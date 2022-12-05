package web

import (
	"DistributedDB/db"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
)

// server contains HTTP method handlers to be used for the database
type Server struct {
	db         *db.Database
	shardIdx   int
	shardCount int
	shardAddr  map[int]string
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values
func NewServer(db *db.Database, shardIdx int, shardCount int, shardAddr map[int]string) *Server {
	return &Server{
		db:         db,
		shardIdx:   shardIdx,
		shardCount: shardCount,
		shardAddr:  shardAddr,
	}
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.shardAddr[shard] + r.RequestURI
	fmt.Fprintf(w, "redirecting from shard %d to shard %d (%q)\n", s.shardIdx, shard, url)
	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redircting the request: %v", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

// GeHandler handlers "get" endpoints
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	value, err := s.db.GetKey(key)

	shard := s.getShard(key)

	if shard != s.shardIdx {
		s.redirect(shard, w, r)
		return
	}
	if err != nil {
		fmt.Fprintf(w, "GetKey error, the key is %s, err: %v\n", key, err)
	}
	if value == nil {
		fmt.Fprintf(w, "there is no value of the key: %s\n", key)
	} else {
		fmt.Fprintf(w, "the value of the key: %s is %s\n", key, value)
	}
}

func (s *Server) getShard(key string) int {
	h := fnv.New64()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.shardCount))
}

// GeHandler handlers "set" endpoints
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.getShard(key)

	if shard != s.shardIdx {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))
	if err != nil {
		fmt.Fprintf(w, "SetKey error, the key is %s, err: %v, shardIdx is %d\n", key, err, shard)
	}
	fmt.Fprintf(w, "Set key success!, shardIdx is %d\n", shard)
}

// ListenAndServe starts to accept the requests
func (s *Server) ListenAndServe() error {
	log.Printf("Server address is %s", s.shardAddr[s.shardIdx])
	return http.ListenAndServe(s.shardAddr[s.shardIdx], nil)
}
