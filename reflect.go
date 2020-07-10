package db

import (
	"errors"
	"reflect"
)

func parseInterface(p interface{}) (tableName string, interfacevalue, structValue reflect.Value, structType reflect.Type, err error) {
	value := reflect.ValueOf(p)
	switch value.Kind() {
	case reflect.Struct:
		structValue = value
		if table, ok := p.(TableNameble); ok {
			tableName = table.TableName()
		}
	case reflect.Ptr:
		ptrValue := reflect.Indirect(value)
		switch ptrValue.Kind() {
		case reflect.Slice:
			interfacevalue = ptrValue
			sliceElementType := interfacevalue.Type().Elem()
			switch sliceElementType.Kind() {
			case reflect.Ptr:
				structType = sliceElementType.Elem()
				if structType.Kind() == reflect.Struct && structType.Name() != "Time" {
					pv := reflect.New(structType)
					tableName = parseTableNameBySlice(sliceElementType, pv)
				} else {
					return
				}
			case reflect.Struct:
				structType = sliceElementType
				pv := reflect.Indirect(reflect.New(structType))
				tableName = parseTableNameBySlice(sliceElementType, pv)
			default:
				structType = sliceElementType
				return
			}

		case reflect.Struct:
			structType = ptrValue.Type()
			interfacevalue = ptrValue
			if ptrValue.Type().Name() != "Time" {
				structValue = ptrValue
				if table, ok := p.(TableNameble); ok {
					tableName = table.TableName()
				}
			}
		default: //接受值的指针
			structType = ptrValue.Type()
			interfacevalue = ptrValue
			return
		}
	case reflect.Slice:
		interfacevalue = value
		sliceElementType := interfacevalue.Type().Elem()
		switch sliceElementType.Kind() {
		case reflect.Ptr:
			structType = sliceElementType.Elem()
			if structType.Kind() == reflect.Struct {
				pv := reflect.New(structType)
				tableName = parseTableNameBySlice(sliceElementType, pv)
			} else {
				return
			}
		case reflect.Struct:
			structType = sliceElementType
			pv := reflect.Indirect(reflect.New(structType))
			tableName = parseTableNameBySlice(sliceElementType, pv)
		}
	default:
		err = errors.New("unsurpported type")
		return
	}
	return
}

func parseTableNameBySlice(t reflect.Type, vs ...reflect.Value) string {
	method, ok := t.MethodByName("TableName")
	if ok {
		var args = []reflect.Value{}
		if len(vs) > 0 {
			args = append(args, vs[0])
		}
		v := method.Func.Call(args)
		if len(v) > 0 {
			return v[0].String()
		}
	}
	if t.Kind() == reflect.Ptr {
		return toSnake(t.Elem().Name())
	}
	return toSnake(t.Name())
}
