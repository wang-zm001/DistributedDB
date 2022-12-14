package lru

import "container/list"

type Cache struct {
	maxBytes int64
	nBytes   int64
	list     *list.List
	// value is the pointer of list element
	cache map[string]*list.Element   
	// optional and executed when an entry is purged
	OnEvicted func(key string, value []byte)
}

// list entry
type entry struct {
	key   string
	value []byte
}

func NewCache(maxBytes int64, onEvicted func(string, []byte)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		list:      list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

// Get returns the value and move the element to front if the key exists
func (c *Cache) Get(key string) (value []byte, ok bool) {
	if ele, ok := c.cache[key]; ok {
		c.list.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return nil, false
}

// RemoveOldestElement removes the oldest element
func (c *Cache) RemoveOldestElement() {
	ele := c.list.Back()
	if ele != nil {
		c.list.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nBytes -= (int64(len(kv.value)) + int64(len(kv.key)))
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add updates ot adds key and value, put the element to front
func (c *Cache) Add(key string, value []byte) {
	if ele, ok := c.cache[key]; ok {
		c.list.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.nBytes = c.nBytes - int64(len(kv.value)) + int64(len(value))
		kv.value = value
	} else {
		ele := c.list.PushFront(&entry{key: key, value: value})
		c.cache[key] = ele
		c.nBytes += (int64(len(value)) + int64(len(key)))
	}
	for c.maxBytes != 0 && c.maxBytes < c.nBytes {
		c.RemoveOldestElement()
	}
}

func (c *Cache) Len() int {
	return c.list.Len()
}
