// Extract data from reflect.Value into a interface
// It's NOT reflect.Value.Interface(). The method only valid on struct but
// a basic type like int64 
// Limit by refelct.Value, only convert to int64, float64, string, 
// and collections or struct
package pgears

import (
	"fmt"
	"reflect"
	"encoding/json"
)

func SelectExtractor(val reflect.Value) func(reflect.Value)interface{} {
	var v = val
	var typ = v.Type()
	if typ.Kind() == reflect.Ptr {
		var v = reflect.Indirect(v)
		typ = v.Type()
	}
	var ret func(reflect.Value)interface{}
	switch typ.Kind() {
	case reflect.Bool:
		ret = ExtractBool
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		ret = ExtractInt
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		ret = ExtractUint
	case reflect.Float64, reflect.Float32:
		ret = ExtractFloat
	case reflect.String:
		ret = ExtractString
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Chan, reflect.Func, reflect.Interface:
		ret = ExtractObject
	default:
		var message = fmt.Sprintf("I don't know how to extract a %v", v)
		panic(message)
	}
	return ret
}

func ExtractField(val reflect.Value, field reflect.StructField) interface{}{
	var itf interface{} = Extract(val)
	if field.Tag.Get("jsonto") != "" {
		itf, _ = json.Marshal(itf)
	}
	return itf
}

func Extract(val reflect.Value) interface{} {
	var v = val
	var typ = v.Type()
	if typ.Kind() == reflect.Ptr {
		var v = reflect.Indirect(v)
		typ = v.Type()
	}
	var ret interface{}
	switch typ.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		ret = val.Int()
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		ret = val.Uint()
	case reflect.Float64, reflect.Float32:
		ret = val.Float()
	case reflect.String:
		ret = val.String()
	case reflect.Complex64, reflect.Complex128:
		ret = val.Complex()
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Chan, reflect.Func, reflect.Interface:
		ret = val.Interface()
	default:
		var message = fmt.Sprintf("I don't know how to extract a %v", v)
		panic(message)
	}
	return ret
}

func ExtractBool(val reflect.Value) (ret interface{}) {
	ret = val.Bool()
	return ret
}

func ExtractInt(val reflect.Value) (ret interface{}) {
	ret = val.Int()
	return ret
}

func ExtractUint(val reflect.Value) (ret interface{}) {
	ret = val.Uint()
	return ret
}

func ExtractFloat(val reflect.Value) (ret interface{}) {
	ret = val.Float()
	return ret
}

func ExtractString(val reflect.Value) (ret interface{}) {
	ret = val.String()
	return ret
}
// all object can be box to a interface{}
func ExtractObject(val reflect.Value) (ret interface{}) {
	ret = val.Interface()
	return ret
}

func ExtractJsonMap(val reflect.Value) (ret interface{}) {
	ret, _ = json.Marshal(val.Interface())
	return ret
}
