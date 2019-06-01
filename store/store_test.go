package store

import (
	"bytes"
	"strings"
	"testing"

	"github.com/danch/danchbase/meta"
	"github.com/google/uuid"
)

func TestAdd(t *testing.T) {
	var table = meta.NewTable("test", 32768)
	var store = NewStore(table, "", "")

	var rec = meta.NewRecord("arbitrary", []byte("value"))
	store.Add(rec)
	var rec2 = store.Get("arbitrary")
	if rec2 == nil {
		t.Error("Lookup failed")
	}
	if strings.Compare(rec.Key(), rec2.Key()) != 0 {
		t.Error("Lookup returned wrong record by key")
	}
	if bytes.Compare(rec.Value(), rec2.Value()) != 0 {
		t.Error("Lookup returned wrong record by value")
	}
}

func TestGet(t *testing.T) {
	var table = meta.NewTable("test", 32768)
	var store = NewStore(table, "", "")

	var rec = meta.NewRecord("arbitrary", []byte("value"))
	store.Add(rec)

	addJunk(store)

	var rec2 = store.Get("arbitrary")
	if rec2 == nil {
		t.Error("Lookup failed")
	}
	if strings.Compare(rec.Key(), rec2.Key()) != 0 {
		t.Error("Lookup returned wrong record by key")
	}
	if bytes.Compare(rec.Value(), rec2.Value()) != 0 {
		t.Error("Lookup returned wrong record by value")
	}
}

func addJunk(store *Store) {
	for i := 0; i < 10; i = i + 1 {
		var key = uuid.New()
		var rec = meta.NewRecord(key.String(), []byte("value"))
		store.Add(rec)
	}
}
