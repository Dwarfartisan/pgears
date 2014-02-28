// extractor.go 中都是用于将反射对象的内含值提取为 interface{} 的工具函数。
// 这些函数在使用 pgears 的时候通常不会直接用到。将其公开主要是考虑这种功能可能会
// 广泛应用于我们的项目，不妨作为一种通用工具提供出来。每个函数的业务并不复杂，只是
// 提取对应类型的值。这里我们准备了两种操作，一个是直接返回 interface{} ，一个是
// 返回对应其类型的 Extractor ，后一个可以允许将类型解析操作 Prepare，提高运行
// 效率
// 
// It's NOT reflect.Value.Interface(). The method only valid on struct but
// a basic type like int64 
// 
// Limit by refelct.Value, only convert to int64, float64, string, 
// and collections or struct
package pgears

import (
	"fmt"
	"reflect"
	"encoding/json"
)


// SelectExtractor 是 Extractor 的选择器。
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

// 这个是动态选择的封装接口。除了内置类型的extract，它还提供了对json类型的提取。
func ExtractField(val reflect.Value, field reflect.StructField) interface{}{
	itf := Extract(val)
	if field.Tag.Get("jsonto") != "" {
		j, err := json.Marshal(itf)
		if err != nil {
			panic(err)
		}
		return j
	}
	return itf
}

// Extract 函数是针对内置类型及其指针的 Extractor
func Extract(val reflect.Value) interface{} {
	var v = val
	var typ = v.Type()
	if typ.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		v = reflect.Indirect(v)
		typ = v.Type()
	}
	var ret interface{}
	switch typ.Kind() {
	case reflect.Bool:
		ret = v.Bool()
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		ret = v.Int()
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		ret = v.Uint()
	case reflect.Float64, reflect.Float32:
		ret = v.Float()
	case reflect.String:
		ret = v.String()
	case reflect.Complex64, reflect.Complex128:
		ret = v.Complex()
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Chan, reflect.Func, reflect.Interface:
		ret = v.Interface()
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

func ExtractBoolPtr(val reflect.Value) (ret interface{}) {
	ret = reflect.Indirect(val).Bool()
	return ret
}

func ExtractIntPtr(val reflect.Value) (ret interface{}) {
	ret = reflect.Indirect(val).Int()
	return ret
}

func ExtractUintPtr(val reflect.Value) (ret interface{}) {
	ret = reflect.Indirect(val).Uint()
	return ret
}

func ExtractFloatPtr(val reflect.Value) (ret interface{}) {
	ret = reflect.Indirect(val).Float()
	return ret
}

func ExtractStringPtr(val reflect.Value) (ret interface{}) {
	ret = reflect.Indirect(val).String()
	return ret
}

// all object can be box to a interface{}
func ExtractObjectPtr(val reflect.Value) (ret interface{}) {
	ret = reflect.Indirect(val).Interface()
	return ret
}

func ExtractJsonMapPtr(val reflect.Value) (ret interface{}) {
	ret, _ = json.Marshal(reflect.Indirect(val).Interface())
	return ret
}
