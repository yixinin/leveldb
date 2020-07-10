package db

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/gjson"
)

type Entity interface {
	// Marshal() string
	// Unmarshal(v string) Entity
	TableName() string
	// Key() string
}

var LevelDB = NewLevelClient("leveldb")

type LevelClient struct {
	db *leveldb.DB
}

func Json() {
	gjson.ParseBytes([]byte(""))
}

func NewLevelClient(path string) *LevelClient {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		panic(err)
	}
	return &LevelClient{
		db: db,
	}
}

var ErrorNoRows = leveldb.ErrNotFound
var ErrorNotPtr = errors.New("not ptr")
var ErrorNotSlice = errors.New("not slice")
var ErrorNotStruct = errors.New("not struct")
var ErrorUniqueDuplicate = errors.New("duplicate unique key")
var ErrorPkDuplicate = errors.New("duplicate unique key")

var locker sync.Mutex

func (c *LevelClient) GenId(tableName string) int {
	locker.Lock()
	defer locker.Unlock()
	var key = []byte(fmt.Sprintf("id/%s", tableName))
	has, err := c.db.Has(key, nil)
	if err != nil {
		return -1
	}
	var nid = 1
	if has {
		buf, err := c.db.Get(key, nil)
		if err != nil {
			return -1
		}
		id, _ := strconv.Atoi(string(buf))
		nid = id + 1
	}
	err = c.db.Put(key, []byte(strconv.Itoa(nid)), nil)
	if err != nil {
		return -1
	}
	return nid
}
