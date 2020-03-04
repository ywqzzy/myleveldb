package db

import (
	"fmt"
	"testing"
)

func Test_DB(t *testing.T) {
	db := Open()

	db.Put([]byte("123"), []byte("456"))

	value, err := db.Get([]byte("123"))
	if err != nil {
		fmt.Println("未知错误")
	}
	fmt.Println(string(value))

	db.Delete([]byte("123"))
	value, err = db.Get([]byte("123"))
	fmt.Println(err)
	fmt.Println(value)

	db.Put([]byte("123"), []byte("789"))
	value, _ = db.Get([]byte("123"))
	fmt.Println(string(value))
}
