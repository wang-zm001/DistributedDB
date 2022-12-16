package zookeeper

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ZkNode struct {
	Path string
	Name string
	Host string
}

const defaultPath = "/DistributedDB"

func Connect(servers []string) (conn *zk.Conn, err error) {
	conn, _, err = zk.Connect(servers,time.Second * 5)
	if err != nil {
		 return nil, err
	}
	return conn, nil
}

func AddZkNode(node *ZkNode, conn *zk.Conn) error {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(node)
	if err != nil {
		return err
	}
	
	var flag int32 = 0
	ACL := zk.WorldACL(zk.PermAll)
	s, err := conn.Create(defaultPath + node.Path, buf.Bytes(), flag, ACL)
	if err != nil {
		return err
	}
	log.Printf("AddZkNod success, %s", s)
	return nil
}

func GetZkNode(path string, conn *zk.Conn, to interface{}) (err error ) {
	data, _, err := conn.Get(defaultPath + path)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}

func GetAllZkNode(conn *zk.Conn) (nodes []ZkNode, err error) {
	childern, _, err := conn.Children(defaultPath)
	if err != nil {
		return nil, err
	}
	for _, child := range childern {
		var node ZkNode
		if err = GetZkNode(defaultPath + child, conn, &node); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, err
}

func IsExist(conn *zk.Conn, path string) (bool, error) {
	isExist, _, err := conn.Exists(defaultPath + path)
	if err != nil {
		return false, err
	}
	return isExist, nil
}