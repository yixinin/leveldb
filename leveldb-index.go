package db

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/bson"
)

var timelayout = "2006-01-02 15:04:05"
var jsonTimeLayout = "2006-01-02T15:04:05"

func (c *LevelClient) insertIndex(v reflect.Value, tableName string, idStr string) error {
	indexs := indexMap[tableName]
	for _, index := range indexs {
		var indexValue = v.FieldByName(index).Interface()
		var key = []byte(fmt.Sprintf("index/%s/%s/%v", tableName, toLower(index), toString(indexValue)))
		value, err := c.db.Get(key, nil)
		if err != nil && err != leveldb.ErrNotFound {
			return err
		}
		if len(value) > 0 {
			pks := strings.Split(string(value), ",")
			pks = append(pks, idStr)
			value = []byte(strings.Join(pks, ","))
		} else {
			value = []byte(idStr)
		}

		err = c.db.Put(key, value, nil)
		if err != nil {
			return err
		}
	}

	uniques := uniqueMap[tableName]
	for _, unique := range uniques {
		var uniqueValue = v.FieldByName(unique).Interface()
		var key = []byte(fmt.Sprintf("index/%s/%s/%v", tableName, toLower(unique), toString(uniqueValue)))
		has, err := c.db.Has(key, nil)
		if err != nil {
			return err
		}
		if has {
			return ErrorUniqueDuplicate
		}
		err = c.db.Put(key, []byte(idStr), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *LevelClient) deleteIndex(key []byte, tableName string) error {
	//查找记录
	buf, err := c.db.Get(key, nil)
	if err != nil {
		return err
	}
	o := gjson.ParseBytes(buf)
	//删除索引
	var indexs = indexMap[tableName]
	var uniques = uniqueMap[tableName]

	for _, index := range indexs {
		var indexValue = o.Get(index).String()
		var key = []byte(fmt.Sprintf("index/%s/%s/%v", tableName, toLower(index), toString(indexValue)))
		err := c.db.Delete(key, nil)
		if err != nil {
			return err
		}
	}
	for _, unique := range uniques {
		var uniqueValue = o.Get(unique).String()
		var key = []byte(fmt.Sprintf("index/%s/%s/%v", tableName, toLower(unique), toString(uniqueValue)))
		err := c.db.Delete(key, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// eq ne
// in nin
// lt lte gt gte
// or
func (c *LevelClient) parseWhere(tableName, pk string, where bson.M) (string, []string, error) {

	var l1Ids = make([][]string, 0)
	var or = false
	var and = false
	for k, v := range where {
		switch k {
		case "$or":
			or = true
			ms, ok := v.([]bson.M)
			if !ok {
				return "", nil, errors.New("unsurpported query syntax")
			}
			for _, m := range ms {
				var _, ids, err = c.parseWhere(tableName, pk, m)
				if err == leveldb.ErrNotFound {
					continue
				}
				if err != nil {
					return "", nil, err
				}
				l1Ids = append(l1Ids, ids)
			}
		default:
			and = true
			var key []byte
			var keyPrefix = fmt.Sprintf("index/%s/%s/", tableName, k)
			var pkPrefix = fmt.Sprintf("id/%s/", tableName)
			switch value := v.(type) {
			case bson.M:
				var startKey, endKey []byte
				var start, end bool
				l2Ids := make([][]string, 0)
				for k1, v1 := range value {
					if k1[0] != '$' {
						return "", nil, errors.New("unsurpported query syntax")
					}
					switch k1 {
					case "$in":
						ss := toStringSlice(v1)
						if StringEq(k, pk) {
							l2Ids = append(l2Ids, ss)
						} else {
							for _, s := range ss {
								var key = []byte(fmt.Sprintf("index/%s/%s/%s", tableName, k, s))
								// 查找key
								buf, err := c.db.Get(key, nil)
								if err == leveldb.ErrNotFound {
									continue
								}
								if err != nil {
									return "", nil, err
								}
								var ids = strings.Split(string(buf), ",")
								l2Ids = append(l2Ids, ids)
							}
						}

					case "$nin":
						ss := toStringSlice(v1)
						iter := c.db.NewIterator(nil, nil)
						if StringEq(k, pk) {
							for ok := iter.Seek([]byte(pkPrefix)); ok; ok = iter.Next() {
								var cKey = string(iter.Key())
								if !strings.HasPrefix(cKey, string([]byte(pkPrefix))) {
									break
								}
								if !hasSuffixInSlice(cKey, ss) {
									cKeys := strings.Split(cKey, "/")
									l2Ids = append(l2Ids, []string{cKeys[len(cKeys)-1]})
								}
							}
						} else {
							for ok := iter.Seek([]byte(keyPrefix)); ok; ok = iter.Next() {
								var cKey = string(iter.Key())
								if !strings.HasPrefix(cKey, string([]byte(keyPrefix))) {
									break
								}
								if !hasSuffixInSlice(cKey, ss) {
									var ids = strings.Split(string(iter.Value()), ",")
									l2Ids = append(l2Ids, ids)
								}
							}
						}

					case "$ne":
						iter := c.db.NewIterator(nil, nil)
						if StringEq(k, pk) {
							for ok := iter.Seek([]byte(pkPrefix)); ok; ok = iter.Next() {
								var cKey = string(iter.Key())
								if !strings.HasPrefix(cKey, string([]byte(pkPrefix))) {
									break
								}
								if !strings.HasSuffix(cKey, toString(v1)) {
									cKeys := strings.Split(cKey, "/")
									l2Ids = append(l2Ids, []string{cKeys[len(cKeys)-1]})
								}
							}
						} else {
							for ok := iter.Seek([]byte(keyPrefix)); ok; ok = iter.Next() {
								var cKey = string(iter.Key())
								if !strings.HasPrefix(cKey, string([]byte(keyPrefix))) {
									break
								}
								if !strings.HasSuffix(cKey, toString(v1)) {
									var ids = strings.Split(string(iter.Value()), ",")
									l2Ids = append(l2Ids, ids)
								}
							}
						}

					case "$gte":
						if StringEq(k, pk) {
							startKey = []byte(fmt.Sprintf("id/%s/%s", tableName, toString(v1)))
						} else {
							startKey = []byte(keyPrefix + toString(v1))
						}

						start = true
					case "$gt":
						if StringEq(k, pk) {
							startKey = []byte(fmt.Sprintf("id/%s/%s", tableName, toString(v1)))
						} else {
							startKey = []byte(keyPrefix + toString(v1))
						}
					case "$eq":
						if StringEq(k, pk) {
							l1Ids = append(l1Ids, []string{toString(v1)})
						} else {
							key = []byte(keyPrefix + toString(v1))
							// 查找key
							buf, err := c.db.Get(key, nil)
							if err != nil {
								return "", nil, err
							}
							ids := strings.Split(string(buf), ",")
							l1Ids = append(l1Ids, ids)
						}

					case "$lt":
						if StringEq(k, pk) {
							endKey = []byte(fmt.Sprintf("id/%s/%s", tableName, toString(v1)))
						} else {
							endKey = []byte(keyPrefix + toString(v1))
						}

					case "$lte":
						if StringEq(k, pk) {
							endKey = []byte(fmt.Sprintf("id/%s/%s", tableName, toString(v1)))
						} else {
							endKey = []byte(keyPrefix + toString(v1))
						}
						end = true
					default:
						return "", nil, errors.New("unsurpported query syntax")
					}
				}

				var l3Ids = make([]string, 0)
				if len(startKey) > 0 {
					iter := c.db.NewIterator(nil, nil)
					for ok := iter.Seek(startKey); ok; ok = iter.Next() {

						var cKey = string(iter.Key())
						if !StringEq(pk, k) && !strings.HasPrefix(cKey, keyPrefix) {
							break
						}
						if StringEq(pk, k) && !strings.HasPrefix(cKey, pkPrefix) {
							break
						}
						if len(endKey) > 0 {
							if cKey > string(endKey) {
								break
							}
							if cKey == string(endKey) && !end {
								break
							}
						}
						if !start && cKey == string(startKey) {
							continue
						}
						if StringEq(pk, k) {
							cKeys := strings.Split(cKey, "/")
							l3Ids = append(l3Ids, cKeys[len(cKeys)-1])
						} else {
							var ids = strings.Split(string(iter.Value()), ",")
							l3Ids = append(l3Ids, ids...)
						}
					}
				} else if len(endKey) > 0 {
					iter := c.db.NewIterator(nil, nil)
					var i = 0
					for ok := iter.Seek(endKey); ok; ok = iter.Prev() {
						var cKey = string(iter.Key())
						if i == 0 {
							i++
							if !end {
								continue
							}
							if !strings.HasPrefix(cKey, keyPrefix) {
								continue
							}
						}

						if !StringEq(pk, k) && !strings.HasPrefix(cKey, keyPrefix) {
							break
						}
						if StringEq(pk, k) && !strings.HasPrefix(cKey, pkPrefix) {
							break
						}
						if StringEq(pk, k) {
							cKeys := strings.Split(cKey, "/")
							l3Ids = append(l3Ids, cKeys[len(cKeys)-1])
						} else {
							var ids = strings.Split(string(iter.Value()), ",")
							l3Ids = append(l3Ids, ids...)
						}
					}
				}
				if len(l3Ids) > 0 {
					l2Ids = append(l2Ids, l3Ids)
				}
				if len(l2Ids) > 0 {
					var ids = andSlice(l2Ids)
					l1Ids = append(l1Ids, ids)
				}
			case string, time.Time, int, int32, int64, float32, float64, bool:
				if StringEq(k, pk) {
					l1Ids = append(l1Ids, []string{fmt.Sprintf("%d", value)})
				} else {
					key = []byte(keyPrefix + toString(value))
					// 查找key
					buf, err := c.db.Get(key, nil)
					if err != nil {
						return "", nil, err
					}
					ids := strings.Split(string(buf), ",")
					l1Ids = append(l1Ids, ids)
				}
			default:
				return "", nil, errors.New("unsurpported query syntax")
			}
		}

	}
	if and && or {
		return "", nil, errors.New("unsurpported both '$and' and '$or'")
	}
	if and {
		var ids = andSlice(l1Ids)
		var id string
		if len(ids) > 0 {
			id = ids[0]
		}
		return id, ids, nil
	}
	if or {
		var ids = orSlice(l1Ids)
		var id string
		if len(ids) > 0 {
			id = ids[0]
		}
		return id, ids, nil
	}
	return "", nil, ErrorNoRows
}

func andSlice(ss [][]string) []string {
	if len(ss) == 0 {
		return []string{}
	}
	if len(ss) == 1 {
		return ss[0]
	}
	var slice = make([]string, 0)
	var m = make(map[string]bool)
	for i := 1; i < len(ss); i++ {
		for _, v := range ss[i] {
			if inSlice(v, ss[i-1]) {
				m[v] = true
			}
		}
	}
	for k := range m {
		slice = append(slice, k)
	}
	return slice
}

func orSlice(ss [][]string) []string {
	if len(ss) == 0 {
		return []string{}
	}
	if len(ss) == 1 {
		return ss[0]
	}
	var slice = make([]string, 0)
	var m = make(map[string]bool)
	for i := 0; i < len(ss); i++ {
		for _, v := range ss[i] {
			m[v] = true
		}
	}
	for k := range m {
		slice = append(slice, k)
	}
	return slice
}

func toString(i interface{}) string {
	switch v := i.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("%011d", v)
	case int32:
		return fmt.Sprintf("%011d", v)
	case int64:
		return fmt.Sprintf("%011d", v)
	case float32:
		return fmt.Sprintf("%011.4f", v)
	case float64:
		return fmt.Sprintf("%011.4f", v)
	case bool:
		return fmt.Sprintf("%v", v)
	case time.Time:
		return v.Format(timelayout)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toStringSlice(i interface{}) []string {
	switch value := i.(type) {
	case []string:
		return value
	case []time.Time:
		var s = make([]string, 0, len(value))
		for _, v := range value {
			s = append(s, v.Format(timelayout))
		}
		return s
	case []int:
		var s = make([]string, 0, len(value))
		for _, v := range value {
			s = append(s, toString(v))
		}
		return s
	case []int32:
		var s = make([]string, 0, len(value))
		for _, v := range value {
			s = append(s, toString(v))
		}
		return s
	case []int64:
		var s = make([]string, 0, len(value))
		for _, v := range value {
			s = append(s, toString(v))
		}
		return s
	case []float32:
		var s = make([]string, 0, len(value))
		for _, v := range value {
			s = append(s, toString(v))
		}
		return s
	case []float64:
		var s = make([]string, 0, len(value))
		for _, v := range value {
			s = append(s, toString(v))
		}
		return s
	default:
		return []string{}
	}
}

func inSlice(s string, ss []string) bool {
	for _, v := range ss {
		if s == v {
			return true
		}
	}
	return false
}

func hasSuffixInSlice(s string, ss []string) bool {
	for _, v := range ss {
		if strings.HasSuffix(s, v) {
			return true
		}
	}
	return false
}
