package meta

// Table describes a table in the system
type Table struct {
	name        string
	segmentSize int32
}
func (table Table) Name() string {
	return table.name
}

// NewTable creates a table given the name and a segmentSize
func NewTable(name string, segSize int32) *Table {
	var t = new(Table)
	t.name = name
	t.segmentSize = segSize
	return t
}
