package db

// func (c *LevelClient) parseWhere(tableName string, where map[string]interface{}) (string, []string, error) {

// 	var indexs = indexMap[tableName]
// 	var uniques = uniqueMap[tableName]

// 	var idWheres = make([][]string, 0)
// 	var i = 0
// 	for k, v := range where {
// 		k = toLower(k)
// 		if k == "id" {
// 			id, _ := v.(int)
// 			if id > 0 {
// 				var idStr = strconv.Itoa(id)
// 				return idStr, []string{idStr}, nil
// 				break
// 			}
// 		}
// 		var ids = make([]string, 0)
// 		if Contains(indexs, k) {
// 			var key = []byte(fmt.Sprintf("index/%s/%s/%v", tableName, k, v))

// 			buf, err := c.db.Get(key, nil)
// 			if err != nil {
// 				return "", nil, err
// 			}
// 			var ss = strings.Split(string(buf), ",")
// 			if i == 0 {
// 				ids = append(ids, ss...)
// 			} else { //必须要上一个条件中的才有效
// 				for _, id := range ss {
// 					if Contains(idWheres[i-1], id) {
// 						ids = append(ids, id)
// 					}
// 				}
// 			}

// 		} else if Contains(uniques, k) {
// 			var key = []byte(fmt.Sprintf("index/%s/%s/%v", tableName, k, v))
// 			buf, err := c.db.Get(key, nil)
// 			if err != nil {
// 				return "", nil, err
// 			}
// 			var id = string(buf)
// 			if i == 0 {
// 				ids = append(ids, id)
// 			} else {
// 				if Contains(idWheres[i-1], id) {
// 					ids = append(ids, id)
// 				}
// 			}
// 		}
// 		idWheres = append(idWheres, ids)
// 		i++
// 	}

// 	if len(idWheres) > 0 {
// 		var ids = idWheres[len(idWheres)-1]
// 		for _, v := range ids {
// 			if v != "" {
// 				return v, ids, nil
// 			}
// 		}
// 	}
// 	return "", nil, ErrorNoRows
// }
