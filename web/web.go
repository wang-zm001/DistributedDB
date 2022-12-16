package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/wang-zm001/DistributedDB/consistenthash"
	"github.com/wang-zm001/DistributedDB/db"
	"github.com/wang-zm001/DistributedDB/replication"
	"github.com/wang-zm001/DistributedDB/zookeeper"
)

// server contains HTTP method handlers to be used for the database
type Server struct {
	db       *db.Database
	conn     *zk.Conn
	addr     string
	shard    string
	ShardMap *consistenthash.Map
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values
func NewServer(db *db.Database, conn *zk.Conn, addr string, shard string) *Server {
	nodes, err := zookeeper.GetAllZkNode(conn)
	if err != nil {
		return nil
	}
	var addrs []string
	for _, node := range nodes {
		addrs = append(addrs, node.Host)
	}
	sMap := consistenthash.New(3, nil)
	sMap.Add(addrs...)

	return &Server{
		db:       db,
		addr:     addr,
		conn:     conn,
		shard:    shard,
		ShardMap: sMap,
	}
}

func (s *Server) redirect(addr string, w http.ResponseWriter, r *http.Request) {
	url := "http://" + addr + r.RequestURI
	fmt.Fprintf(w, "redirecting from shard %s to shard %s (%q)\n", s.shard, addr, url)
	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redircting the request: %v\n", err)
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

	addr := s.ShardMap.Get(key)

	if addr != s.addr {
		s.redirect(addr, w, r)
		return
	}

	if err != nil {
		fmt.Fprintf(w, "GetKey error, the key is %s, err: %v\n", key, err)
	}
	if value == nil {
		fmt.Fprintf(w, "There is no value of the key: %s\n", key)
	} else {
		fmt.Fprintf(w, "The value of the key: %s is %s\n", key, value)
	}
}

// GeHandler handlers "set" endpoints
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	addr := s.ShardMap.Get(key)

	if addr != s.addr {
		s.redirect(addr, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))
	if err != nil {
		fmt.Fprintf(w, "SetKey error, the key is %s, err: %v, shard is %s\n", key, err, s.shard)
	}
	fmt.Fprintf(w, "Set key success!, shard is %s\n", s.shard)
}

// DeleteExtraKeys delete keys that don't belong to the current shard.
func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v\n", s.db.DeleteExtraKeys(func(key string) bool {
		return s.ShardMap.Get(key) != s.addr
	}))
}

// GetNextKeyForReplication returns the next key for replication.
func (s *Server) GetNextKeyForReplication(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	k, v, err := s.db.GetNextKeyForReplication()
	enc.Encode(&replication.NexKeyValue{
		Key:   string(k),
		Value: string(v),
		Err:   err,
	})
}

// DeleteExtraKeys delete the key from replica queue
func (s *Server) DeleteReplicationKey(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	key := r.Form.Get("key")
	value := r.Form.Get("value")

	err := s.db.DeleteReplicationKey([]byte(key), []byte(value))
	if err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
		fmt.Fprintf(w, "error: %v", err)
		return
	}
	fmt.Fprintf(w, "ok")
}
