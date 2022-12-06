package db

import (
	"fmt"

	bolt "github.com/boltdb/bolt"
)

var (
	defaultBucket = []byte("default")
)

type Database struct {
	db *bolt.DB
}

// NewDatabase returns an instance of a batabase that we can work with
func NewDatabase(dataPath string) (db *Database, close func() error, err error) {
	boltDB, err := bolt.Open(dataPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{db: boltDB}
	closeFunc := boltDB.Close

	if err := db.createDefaultBucket(); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

func (d *Database) createDefaultBucket() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucket)
		return err
	})
}

// SetKey sets the key to the requested value or return an error
func (d *Database) SetKey(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.Put([]byte(key), value)
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
