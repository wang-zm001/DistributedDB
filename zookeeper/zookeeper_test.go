package zookeeper

import (
	"log"
	"reflect"
	"testing"
)

func TestAdd(t *testing.T){
	servers := []string{"127.0.0.1:2181"}
	conn, err := Connect(servers)
	if err != nil {
		log.Fatalf("Connect err, %v", err)
	}
	defer conn.Close()
	node := &ZkNode {
		Path: "/DistributedDB/db0",
		Name: "db0",
		Host: "127.0.0.1:8080",
	}
	err = AddZkNode(node, conn)
	if err != nil {
		log.Fatalf("AddZkNode err, %v", err)
	}
}

func TestGet(t *testing.T) {
	servers := []string{"127.0.0.1:2181"}
	conn, err := Connect(servers)
	if err != nil {
		log.Fatalf("Connect err, %v", err)
	}
	defer conn.Close()
	var node ZkNode
	err = GetZkNode("/db0", conn, &node)
	if err != nil {
		log.Fatalf("GetZkNode err, %v", err)
	}
	want := &ZkNode {
		Path: "/db0",
		Name: "db0",
		Host: "127.0.0.1:8080",
	}
	if !reflect.DeepEqual(*want, node) {
		log.Fatalf("Not equal")
	}
}

func TestGetAll(t *testing.T) {
	servers := []string{"127.0.0.1:2181"}
	conn, err := Connect(servers)
	if err != nil {
		log.Fatalf("Connect err, %v", err)
	}
	defer conn.Close()
	// var node ZkNode
	nodes, err := GetAllZkNode(conn)
	if err != nil {
		log.Fatalf("GetZkNode err, %v", err)
	}
	want := []ZkNode{
		{
			Path: "/db0",
			Name: "db0",
			Host: "127.0.0.1:8080",
		},
	}
	if !reflect.DeepEqual(want, nodes) {
		log.Fatalf("Not equal")
	}
}