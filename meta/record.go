package meta

// Record is a record within a table
type Record struct {
	key   string
	value []byte
}

// NewRecord creates a new Record
func NewRecord(key string, value []byte) *Record {
	var r = new(Record)
	r.key = key
	r.value = value
	return r
}

// Key returns the key of the record
func (rec *Record) Key() string {
	return rec.key
}

// Value returns the value of the reocrd
func (rec *Record) Value() []byte {
	return rec.value
}
