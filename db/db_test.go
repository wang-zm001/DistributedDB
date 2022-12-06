package db_test

import (
	"DistributedDB/db"
	"reflect"
	"testing"
)

func TestSetAndGetKey(t *testing.T) {
	dbName := "testDB"
	db, close, err := db.NewDatabase(dbName)
	if err != nil {
		t.Fatalf("Create database fail: %v", err)
	}
	defer close()

	putValue := []byte("testValue")
	err = db.SetKey("testKey", putValue)
	if err != nil {
		t.Fatalf("Set key fail: %v", err)
	}

	getValue, err := db.GetKey("testKey")
	if err != nil {
		t.Fatalf("Get key fail: %v", err)
	}
	if !reflect.DeepEqual(putValue, getValue) {
		t.Fatalf("The value doesn't match, put value is %v, get value is %v", putValue, getValue)
	}
}       