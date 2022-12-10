package db

import (
	"bytes"
	"errors"
	"fmt"

	bolt "github.com/boltdb/bolt"
	cache "github.com/wang-zm001/DistributedDB/cache"
)

var (
	defaultBucket = []byte("default")
	replicaBucket = []byte("replica")
)

type Database struct {
	db       *bolt.DB
	cache    *cache.Cache
	readOnly bool
}

// NewDatabase returns an instance of a batabase that we can work with
func NewDatabase(dataPath string, readOnly bool) (db *Database, close func() error, err error) {
	boltDB, err := bolt.Open(dataPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{db: boltDB, readOnly: readOnly}
	closeFunc := boltDB.Close

	if err := db.createBucket(); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("creating bucket: %w", err)
	}

	return db, closeFunc, nil
}

func (d *Database) createBucket() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(replicaBucket); err != nil {
			return err
		}
		return nil
	})
}

// SetKey sets the key to the requested value or return an error
func (d *Database) SetKey(key string, value []byte) error {
	if d.readOnly {
		return errors.New("can not set key to a read-only database")
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}
		if err := tx.Bucket(replicaBucket).Put([]byte(key), value); err != nil {
			return err
		}
		return nil
	})
}

// SetKeyOnReplication sets the key to the requested value into the default database and
// does not write to the replication queque
// This method is intended to be used only on replicas.
func (d *Database) SetKeyOnReplica(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}
		return nil
	})
}

// GetKey gets the value of the requested from a default database
func (d *Database) GetKey(key string) ([]byte, error) {
	var result []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = b.Get([]byte(key))
		return nil
	})

	if err == nil {
		return result, nil
	}
	return nil, err
}

// GetNextKeyForReplication returns the key and value for the keys that have
// changed and have not yet been applied to replicas.
// If there are no new keys, nil key and value will be returned.
func (d *Database) GetNextKeyForReplication() (key, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)
		k, v := b.Cursor().First()
		key = make([]byte, len(k))
		value = make([]byte, len(v))
		copy(key, k)
		copy(value, v)
		return nil
	})

	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// DeleteReplicationKey delete the keys from the replication queue
// if the value matches the contents or if the key is aready absent.
func (d *Database) DeleteReplicationKey(key, value []byte) (err error) {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)

		v := b.Get(key)
		if v == nil {
			return errors.New("key does not exist")
		}

		if !bytes.Equal(v, value) {
			return errors.New("value does not match")
		}
		return b.Delete(key)
	})
}

// DeleteExtraKeys deletes the keys that not belong to this shard
func (d *Database) DeleteExtraKeys(isExtra func(string) bool) error {
	var keys []string

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, v []byte) error {
			if isExtra(string(k)) {
				keys = append(keys, string(k))
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)

		for _, k := range keys {
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}
