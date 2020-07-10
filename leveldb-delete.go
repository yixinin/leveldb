package db

import (
	"fmt"
	"reflect"
)

func (c *LevelClient) DeleteOne(e interface{}, where map[string]interface{}) error {
	var tableName string
	if tb, ok := e.(TableNameble); ok {
		tableName = tb.TableName()
	}
	var v reflect.Value
	if reflect.TypeOf(e).Kind() == reflect.Ptr {
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
	} else {
		idStr = toString(id)
	}

	var key = []byte(fmt.Sprintf("id/%s/%s", tableName, idStr))
	locker.Lock()
	defer locker.Unlock()
	//删除索引
	if err := c.deleteIndex(key, tableName); err != nil {
		return err
	}
	//删除记录
	return c.db.Delete(key, nil)
}
