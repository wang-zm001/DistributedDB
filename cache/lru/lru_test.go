package lru_test

import (
	"reflect"
	"testing"
	lru "github.com/wang-zm001/DistributedDB/cache/lru"
)

type String string

func (s String) Len() int {
	return len(s)
}

func TestGet(t *testing.T) {
	lru := lru.NewCache(int64(0), nil)
	lru.Add("key1", []byte("1234"))
	if v, ok := lru.Get("key1"); !ok || string(v) != "1234" {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if _, ok := lru.Get("key2"); ok {
		t.Fatalf("cache miss key2 failed")
	}
}

func TestRemoveOldestElement(t *testing.T) {
	k1, k2, k3 := "test-key1","test-key2","test-key3"
	v1, v2, v3 := "test-value1","test-value2","test-value3"

	max := len(k1+k2+v1+v2)
	keys := make([]string, 0)
	cache := lru.NewCache(int64(max), func(key string, value []byte) {
		// t.Logf("The removed key is %s, value is %v", key, value)
		keys = append(keys, key)
	})
	cache.Add(k1, []byte(v1))
	cache.Add(k2, []byte(v2))
	cache.Add(k3, []byte(v3))

	if _, ok := cache.Get("test-key1"); ok || cache.Len() != 2 {
		t.Fatal("Remove test-key1 failed")
	}

	if !reflect.DeepEqual(keys,[]string{"test-key1"}) {
		t.Fatal("Call OnEvicted failed")
	}
}