package db

import (
	"fmt"
	"reflect"
	"strings"
)

// var colMap map[string]map[string]Col
var pkMap map[string]string
var indexMap map[string][]string
var uniqueMap map[string][]string

type Col struct {
	t reflect.Type
	i int
}
type TableNameble interface {
	TableName() string
}

func init() {
	// colMap = make(map[string]map[string]Col, 100)
	pkMap = make(map[string]string, 5)
	indexMap = make(map[string][]string, 5)
	uniqueMap = make(map[string][]string, 5)
}

func StringEq(s1, s2 string) bool {
	s1 = strings.ReplaceAll(s1, "_", "")
	s2 = strings.ReplaceAll(s2, "_", "")
	return strings.ToLower(s1) == strings.ToLower(s2)
}
func Contains(ss []string, s string) bool {
	if len(ss) <= 0 {
		return false
	}
	for _, v := range ss {
		if StringEq(v, s) {
			return true
		}
	}
	return false
}

func toLower(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

func toSnake(str string) string {
	var s = make([]byte, 0, len(str)*2)
	for i, v := range []byte(str) {
		if v >= 'a' {
			s = append(s, v)
		} else if v <= 'z' && v >= 'A' {
			if i > 0 {
				s = append(s, '_')
			}
			s = append(s, v+32)
		}
	}
	return string(s)
}

//Register Register Model
func Register(v interface{}) error {
	var t = reflect.TypeOf(v)

	if t.Kind() == reflect.Ptr {
		t = reflect.ValueOf(v).Elem().Type()
	}
	// c := make(map[string]Col, t.NumField())
	var pk = "Id"
	var indexs = make([]string, 0, 1)
	var uniques = make([]string, 0, 1)
	for i := 0; i < t.NumField(); i++ {
		var f = t.Field(i)
		var name = f.Name
		var tag, ok = f.Tag.Lookup("sql")
		if ok {
			if strings.Contains(tag, "pk") {
				pk = name
			} else if strings.Contains(tag, "index") {
				indexs = append(indexs, name)
			} else if strings.Contains(tag, "unique") {
				uniques = append(uniques, name)
			}
		}
	}
	table, ok := v.(TableNameble)
	if ok {
		var tableName = table.TableName()
		pkMap[tableName] = pk
		indexMap[tableName] = indexs
		uniqueMap[tableName] = uniques
		return nil
	} else {
		var tableName = toSnake(t.Name())
		pkMap[tableName] = pk
		indexMap[tableName] = indexs
		uniqueMap[tableName] = uniques
		return nil
	}
	return fmt.Errorf("please impl `func TableName() string`, struct name:%s", t.Name())
}
