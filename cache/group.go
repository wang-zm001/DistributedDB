package cache

import (
	"fmt"
	"log"
	"sync"
)

type Group struct {
	name      string
	getter    Getter
	mainCache Cache
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
	if value, err = g.getter.Get(key); err != nil {
		return nil, err
	}
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value []byte) {
	g.mainCache.add(key, value)
}
