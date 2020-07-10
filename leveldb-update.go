package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

func (c *LevelClient) UpdateOne(e interface{}, where map[string]interface{}) error {
	var tableName string
	if tb, ok := e.(TableNameble); ok {
		tableName = tb.TableName()
	}
	var v reflect.Value
	var kind = reflect.TypeOf(e).Kind()
	if kind == reflect.Ptr {
		v = reflect.Indirect(reflect.ValueOf(e))
	} else {
		v = reflect.ValueOf(e)
	}
	if tableName == "" {
		tableName = toSnake(v.Type().Name())
	}
	var id int
	var idField = v.FieldByName(pkMap[tableName])

	id = int(idField.Int())

	var idStr string
	if id <= 0 { //通过where查找
		var err error
		idStr, _, err = c.parseWhere(tableName, pkMap[tableName], where)
		if err != nil {
			return err
		}
		if !idField.CanSet() {
			return errors.New("need ptr if id not set")
		}
		id, _ = strconv.Atoi(idStr)
		idField.Set(reflect.ValueOf(id))
	} else {
		idStr = toString(id)
	}

	locker.Lock()
	defer locker.Unlock()
	//查找记录
	var key = []byte(fmt.Sprintf("id/%s/%s", tableName, idStr))
	if has, _ := c.db.Has(key, nil); !has {
		return ErrorNoRows
	}
	//删除索引
	if err := c.deleteIndex(key, tableName); err != nil {
		return err
	}

	//更新索引
	if err := c.insertIndex(v, tableName, idStr); err != nil {
		return err
	}
	//替换数据
	buf, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return c.db.Put(key, buf, nil)
}

func (c *LevelClient) UpdateMulti(v interface{}) error {
	return nil
}
