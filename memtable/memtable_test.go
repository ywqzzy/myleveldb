package memtable

import (
	"fmt"
	"mylevelDB/internal"
	"testing"
)

func Test_MemTable(t *testing.T) {
	memTable := New()

	memTable.Add(1234567, internal.TypeValue, []byte("asdhjkadshkj"), []byte("ahdkjhaskjdj"))

	_, value, _ := memTable.Get([]byte("asdhjkadshkj11"))
	fmt.Println(string(value))
}
