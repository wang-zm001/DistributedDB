package cache

import (
	"fmt"
	"log"
	"sync"

	"github.com/wang-zm001/DistributedDB/cache/singleflight"
	pb "github.com/wang-zm001/DistributedDB/cache/proto"
)

type Group struct {
	name      string
	getter    Getter
	mainCache Cache
	peers     PeerPicker
	loader    *singleflight.Group
}

// A Getter loads data for a key.
type Getter interface {
	Get(key string) ([]byte, error)
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(key string) ([]byte, error)

// Get implements Getter interface function
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("getter is nil.")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: Cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

func GetGroup(name string) *Group {
	mu.RLock()
	defer mu.RUnlock()
	return groups[name]
}

func (g *Group) Get(key string) (value []byte, err error) {
	if key == "" {
		return nil, fmt.Errorf("the key is nil")
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Printf("[cache hit]")
		return v, nil
	}

	// 本地没有找到
	return g.load(key)
}

func (g *Group) load(key string) (value []byte, err error) {
	// return g.getLocally(key)
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	val, err := g.loader.Do(key, func() ([]byte, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}

		return g.getLocally(key)
	})

	if err == nil {
		return val, nil
	}
	return
}

func (g *Group) getLocally(key string) ([]byte, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return nil, err

	}
	value := make([]byte, len(bytes))
	copy(value, bytes)
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value []byte) {
	g.mainCache.add(key, value)
}

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func (g *Group) getFromPeer(peer PeerGetter, key string) ([]byte, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}
