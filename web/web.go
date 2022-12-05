package web

import (
	"DistributedDB/db"
	"fmt"
	"net/http"
)

// server contains HTTP method handlers to be used for the database
type Server struct {
	db *db.Database
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values
func NewServer(db *db.Database) *Server {
	return &Server{
		db: db,
	}
}

// GeHandler handlers "get" endpoints
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	value, err := s.db.GetKey(key)
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

	err := s.db.SetKey(key, []byte(value))
	if err != nil {
		fmt.Fprintf(w, "SetKey error, the key is %s, err: %v\n", key, err)
	}
	fmt.Fprintf(w, "Set key success!\n")
}

// ListenAndServe starts to accept the requests
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(*&addr, nil)
}