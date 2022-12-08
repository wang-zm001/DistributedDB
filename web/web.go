package web

import (
	"github.com/wang-zm001/DistributedDB/config"
	"github.com/wang-zm001/DistributedDB/db"
	"fmt"
	"io"
	"log"
	"net/http"
)

// server contains HTTP method handlers to be used for the database
type Server struct {
	db     *db.Database
	shards *config.Shards
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values
func NewServer(db *db.Database, s *config.Shards) *Server {
	return &Server{
		db:     db,
		shards: s,
	}
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.shards.Addrs[shard] + r.RequestURI
	fmt.Fprintf(w, "redirecting from shard %d to shard %d (%q)\n", s.shards.CurIdx, shard, url)
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

	shard := s.shards.Index(key)

	if shard != s.shards.CurIdx {
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

// GeHandler handlers "set" endpoints
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.shards.Index(key)

	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))
	if err != nil {
		fmt.Fprintf(w, "SetKey error, the key is %s, err: %v, shardIdx is %d\n", key, err, shard)
	}
	fmt.Fprintf(w, "Set key success!, shardIdx is %d\n", shard)
}

// DeleteExtraKeys delete keys that don't belong to the current shard.
func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v", s.db.DeleteExtraKeys(func(key string) bool {
		return s.shards.Index(key) != s.shards.CurIdx
	}))
}

// ListenAndServe starts to accept the requests
func (s *Server) ListenAndServe() error {
	log.Printf("Server address is %s", s.shards.Addrs[s.shards.CurIdx])
	return http.ListenAndServe(s.shards.Addrs[s.shards.CurIdx], nil)
}
