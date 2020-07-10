package db

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"go.mongodb.org/mongo-driver/bson"
)

func (c *LevelClient) FindOne(e interface{}, where map[string]interface{}) error {
	var tableName string
	tb, ok := e.(TableNameble)
	if ok {
		tableName = tb.TableName()
	}
	if reflect.TypeOf(e).Kind() != reflect.Ptr {
		return ErrorNotPtr
	}
	v := reflect.Indirect(reflect.ValueOf(e))
	if tableName == "" {
		tableName = toSnake(v.Type().Name())
	}
	var pk = pkMap[tableName]
	var id = int(v.FieldByName(pk).Int())
	var idStr string

	if id <= 0 {
		var err error
		idStr, _, err = c.parseWhere(tableName, pk, where)
		if err != nil {
			return err
		}
	} else {
		idStr = toString(id)
	}
	if idStr == "" {
		return ErrorNoRows
	}
	var key = []byte(fmt.Sprintf("id/%s/%s", tableName, idStr))
	buf, err := c.db.Get(key, nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, e)
}

func (c *LevelClient) Find(s interface{}, where bson.M) error {
	if reflect.TypeOf(s).Kind() != reflect.Ptr {
		return ErrorNotPtr
	}
	var sliceValue = reflect.Indirect(reflect.ValueOf(s))
	sliceType := sliceValue.Type()
	if sliceType.Kind() != reflect.Slice {
		return ErrorNotSlice
	}

	var isPtr = false
	var tableName string
	var structType reflect.Type
	switch e := sliceType.Elem(); e.Kind() {
	case reflect.Ptr:
		isPtr = true
		structType = e.Elem()
		tableName = parseTableNameBySlice(e, reflect.New(e.Elem()))
	case reflect.Struct:
		structType = e
		tableName = parseTableNameBySlice(e, reflect.Indirect(reflect.New(e)))
	default:
		return ErrorNotStruct
	}

	var pk = pkMap[tableName]
	//按id查找
	var ids = make([]string, 0, sliceValue.Len())
	var validIndexs = make([]int, 0)
	if sliceValue.Len() > 0 {
		for i := 0; i < cap(ids); i++ {
			item := sliceValue.Index(i)
			var idStr string
			if isPtr {
				idStr = toString(reflect.Indirect(item).FieldByName(pk).Interface())
			} else {
				idStr = toString(item.FieldByName(pk).Interface())
			}

			var key = []byte(fmt.Sprintf("id/%s/%s", tableName, idStr))
			buf, err := c.db.Get(key, nil)
			if err != nil {
				if err == leveldb.ErrNotFound {
					continue
				}
				return err
			}
			var m = make(map[string]interface{})
			err = json.Unmarshal(buf, &m)
			if err != nil {
				return err
			}
			if isPtr {
				validIndexs = append(validIndexs, i)
				ptr := reflect.Indirect(item)
				for k, v := range m {
					if x, ok := v.(int64); ok {
						ptr.FieldByName(k).Set(reflect.ValueOf(int(x)))
					} else if x, ok := v.(float64); ok && x == float64(int(x)) {
						ptr.FieldByName(k).Set(reflect.ValueOf(int(x)))
					} else {
						ptr.FieldByName(k).Set(reflect.ValueOf(v))
					}
				}
			}
		}
		if len(validIndexs) > 0 {
			var frist = validIndexs[0]
			var validItem = make([]reflect.Value, 0, len(validIndexs)-1)
			for _, i := range validIndexs {
				if i == frist {
					continue
				}
				validItem = append(validItem, sliceValue.Index(i))
			}
			sliceValue.Set(reflect.Append(sliceValue.Slice(frist, frist+1), validItem...))
		}

		return nil
	}

	if len(ids) <= 0 {
		//where
		if len(where) > 0 {
			var err error
			_, ids, err = c.parseWhere(tableName, pk, where)
			if err != nil {
				return err
			}
		} else {
			//查找全部
			var key = []byte(fmt.Sprintf("id/%s/", tableName))
			iter := c.db.NewIterator(nil, nil)
			for ok := iter.Seek(key); ok; ok = iter.Next() {
				// Use key/value.
				var k = iter.Key()
				var buf = iter.Value()

				if !strings.HasPrefix(string(k), string(key)) {
					return nil
				}

				var item = reflect.New(structType)

				var m = make(map[string]interface{})
				err := json.Unmarshal(buf, &m)
				if err != nil {
					return err
				}
				itemptr := reflect.Indirect(item)
				for k, v := range m {
					if x, ok := v.(int64); ok {
						itemptr.FieldByName(k).Set(reflect.ValueOf(int(x)))
					} else if x, ok := v.(float64); ok && x == float64(int(x)) {
						itemptr.FieldByName(k).Set(reflect.ValueOf(int(x)))
					} else if x, ok := v.(string); ok {
						if x[4] == '-' && x[7] == '-' && x[10] == 'T' {
							t, _ := time.ParseInLocation(jsonTimeLayout, x[:19], time.Local)
							itemptr.FieldByName(k).Set(reflect.ValueOf(t))
						} else {
							itemptr.FieldByName(k).Set(reflect.ValueOf(v))
						}
					} else {
						itemptr.FieldByName(k).Set(reflect.ValueOf(v))
					}

				}
				if isPtr {
					sliceValue.Set(reflect.Append(sliceValue, item))
				} else {
					sliceValue.Set(reflect.Append(sliceValue, reflect.Indirect(item)))
				}

			}
			iter.Release()
			err := iter.Error()
			return err
		}

	}

	for _, id := range ids {
		var key = []byte(fmt.Sprintf("id/%s/%s", tableName, id))
		buf, err := c.db.Get(key, nil)
		if err == leveldb.ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}

		var item = reflect.New(structType)

		var m = make(map[string]interface{})

		err = json.Unmarshal(buf, &m)
		if err != nil {
			return err
		}
		itemptr := reflect.Indirect(item)
		for k, v := range m {
			if x, ok := v.(int64); ok {
				itemptr.FieldByName(k).Set(reflect.ValueOf(int(x)))
			} else if x, ok := v.(float64); ok && x == float64(int(x)) {
				itemptr.FieldByName(k).Set(reflect.ValueOf(int(x)))
			} else if x, ok := v.(string); ok {
				if x[4] == '-' && x[7] == '-' && x[10] == 'T' {
					t, err := time.ParseInLocation(jsonTimeLayout, x[:19], time.Local)
					if err != nil {
						fmt.Println(err)
					}
					itemptr.FieldByName(k).Set(reflect.ValueOf(t))
				} else {
					itemptr.FieldByName(k).Set(reflect.ValueOf(v))
				}
			} else {
				itemptr.FieldByName(k).Set(reflect.ValueOf(v))
			}

		}
		if isPtr {
			sliceValue.Set(reflect.Append(sliceValue, item))
		} else {
			sliceValue.Set(reflect.Append(sliceValue, reflect.Indirect(item)))
		}

	}

	return nil
}
