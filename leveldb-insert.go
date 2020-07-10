package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

func (c *LevelClient) Insert(e interface{}, up ...bool) (int, error) {
	var tableName string
	tb, ok := e.(TableNameble)
	if ok {
		tableName = tb.TableName()
	}
	var v reflect.Value
	if t := reflect.TypeOf(e); t.Kind() == reflect.Ptr {
		v = reflect.Indirect(reflect.ValueOf(e))
	} else {
		v = reflect.ValueOf(e)
	}
	if tableName == "" {
		tableName = toSnake(v.Type().Name())
	}
	var idField = v.FieldByName(pkMap[tableName])
	id := int(idField.Int())
	if id <= 0 {
		id = c.GenId(tableName)
		if !idField.CanSet() {
			return -1, errors.New("need ptr if id not set")
		}
		idField.Set(reflect.ValueOf(id))
	}
	if id <= 0 {
		return 0, errors.New("id生成错误")
	}
	var idStr = toString(id)

	var key = []byte(fmt.Sprintf("id/%s/%s", tableName, idStr))
	var body, err = json.Marshal(e)
	if err != nil {
		return -1, err
	}

	//🔒
	locker.Lock()
	defer locker.Unlock()
	//插入索引和唯一索引
	if err := c.insertIndex(v, tableName, idStr); err != nil {
		return -1, err
	}

	if has, _ := c.db.Has(key, nil); has {
		return -1, ErrorPkDuplicate
	}
	return id, c.db.Put(key, body, nil)
}
