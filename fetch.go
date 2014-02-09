// 用来构造各种类型的数据库字段的填充行为
// 我也打算支持所有长度的整数啊，后来一琢磨最好先细分一下，免得数据填充的时候溢出。
// PG有decimal/numberic到smallint各个级别的数值，smallint就是16字节的，
// 再小就没有了
// 详细文档 http://www.postgresql.org/docs/9.3/static/datatype-numeric.html
// 当然更不幸的是如果从一个自由类型反射回Go的类型，reflect只给提供了有限的几个接口：
// Int（实际是int64，但是不隐式兼容=_=）, Uint（实际是Uint64不解释），Float（实际是 Float64 不解释）
// Bool，String。所以这里弄太复杂也是白瞎。等以后玩熟bytes包再说吧。有兴趣的朋友可以看一下
// pq 的实现，里面有bytes的各种玩法，真正的高端大气。
// 为了兼顾性能和方便，提供两种种版本的fetch：确知 Not Null 的，提供原生类型的直接填充。
// 可能为空的，提供 interface{} 到指针类型的填充，但是取出后调用者要自己判定是否nil，否则 panic 自负～
// 原想给了 NullXxxx 类型的可Scan的数据库类型字段的，可以提供填充，后来一想调用者如果都自己用这一套了
// 也就没 ORM 什么必要了吧……
package pgears

import (
	"fmt"
	"time"
	"encoding/json"
	"reflect"
	"errors"
)

func fetchInt(f *interface{}, to *reflect.Value)error{
	switch val:=(*f).(type) {
	case int64:
		to.SetInt(val)
	case int:
		to.SetInt(int64(val))
	default:
		var message = fmt.Sprintf("%v is't a usable integer", f)
		return errors.New(message)
	}
	return nil
}

func fetchIntPtr(f *interface{}, to *reflect.Value)error{
	if *f == nil {
		to.Set(makeNil(*to))
	}
	switch val:=(*f).(type) {
	case *int64:
		var data int = int(*val)
		to.Set(reflect.ValueOf(&data))
	case *int:
		to.Set(reflect.ValueOf(val))
	default:
		var message = fmt.Sprintf("%v is't a usable integer", f)
		return errors.New(message)
	}
	return nil
}

func fetchInt64(f *interface{}, to *reflect.Value)error{
	switch val:=(*f).(type) {
	case int64:
		to.SetInt(val)
	case int:
		to.SetInt(int64(val))
	default:
		var message = fmt.Sprintf("%v is't a usable int64", f)
		return errors.New(message)
	}
	return nil
}

// 这里的行为不是太确定，应该写个程序试一下。
func fetchInt64Ptr(f *interface{}, to *reflect.Value)error{
	if f == nil {
		to.Set(makeNil(*to))
	}
	switch val:=(*f).(type) {
	case *int64:
		to.Set(reflect.ValueOf(val))
	case *int:
		var data int64 = int64(*val)
		to.Set(reflect.ValueOf(&data))
	default:
		var message = fmt.Sprintf("%v is't a usable int64 pointer", f)
		return errors.New(message)
	}
	return nil
}

func fetchFloat64(f *interface{}, to *reflect.Value)error{
	switch val:=(*f).(type) {
	case float64:
		to.SetFloat(val)
	default:
		var message = fmt.Sprintf("%v is't a usable float64", f)
		return errors.New(message)
	}
	return nil
}

// 这里的行为不是太确定，应该写个程序试一下。
func fetchFloat64Ptr(f *interface{}, to *reflect.Value)error{
	if f == nil {
		to.Set(makeNil(*to))
	}
	switch val:=(*f).(type) {
	case *float64:
		to.Set(reflect.ValueOf(val))
	default:
		var message = fmt.Sprintf("%v is't a usable float64 pointer", f)
		return errors.New(message)
	}
	return nil
}

//其实string取出来的时候是[]byte
func fetchString(f *interface{}, to *reflect.Value)error{
	if v, ok:=(*f).([]byte);ok{
		var data = string(v)
		to.SetString(data)
	}else{
		var message = fmt.Sprintf("%v is't a usable string", f)
		return errors.New(message)
	}
	return nil
}

func fetchStringPtr(f *interface{}, to *reflect.Value)error{
	if f == nil {
		to.Set(makeNil(*to))
	}
	if v, ok:=(*f).([]byte);ok{
		var data = string(v)
		to.Set(reflect.ValueOf(&data))
	}else{
		var message = fmt.Sprintf("%v is't a usable string", f)
		return errors.New(message)
	}
	return nil
}

func fetchBool(f *interface{}, to *reflect.Value)error{
	if v, ok:=(*f).(bool);ok{
		(*to).SetBool(v)
	}else{
		var message = fmt.Sprintf("%v is't a usable bool", f)
		return errors.New(message)
	}
	return nil
}

func fetchBoolPtr(f *interface{}, to *reflect.Value)error{
	if f == nil {
		to.Set(makeNil(*to))
	}
	if v, ok:=(*f).(bool);ok{
		to.Set(reflect.ValueOf(&v))
	}else{
		var message = fmt.Sprintf("%v is't a usable bool ptr", f)
		return errors.New(message)
	}
	return nil
}

func fetchTime(f *interface{}, to *reflect.Value)error{
	if v, ok:=(*f).(time.Time);ok{
		to.Set(reflect.ValueOf(v))
	}else{
		var message = fmt.Sprintf("%v is't a usable time", f)
		return errors.New(message)
	}
	return nil
}

func fetchTimePtr(f *interface{}, to *reflect.Value)error{
	if *f == nil {
		to.Set(makeNil(*to))
	}
	if v, ok:=(*f).(time.Time);ok{
		to.Set(reflect.ValueOf(&v))
	}else{
		var message = fmt.Sprintf("%v is't a usable time ptr", f)
		return errors.New(message)
	}
	return nil
}

// map 本身就是引用对象，不过为了避免出现空指针panic，这里总是会构造一个
// 新的 map
func fetchJsonMap(f *interface{}, to *reflect.Value)error{
	if v, ok:=(*f).([]byte);ok{
		var data interface{}
		json.Unmarshal(v, &data)
		to.Set(reflect.ValueOf(data))
	}else{
		var message = fmt.Sprintf("%v is't a usable json buffer", f)
		return errors.New(message)
	}
	return nil
}

func fetchJsonMapPtr(f *interface{}, to *reflect.Value)error{
	if f == nil {
		to.Set(makeNil(*to))
	}
	if v, ok:=(*f).([]byte);ok{
		var data interface{}
		json.Unmarshal(v, &data)
		to.Set(reflect.ValueOf(&data))
	}else{
		var message = fmt.Sprintf("%v is't a usable json buffer", f)
		return errors.New(message)
	}
	return nil
}

// 为防止出现没有分配空间的情况，这里提供指针和传值两种
func fetchJsonStruct(f *interface{}, to *reflect.Value)error{
	if v, ok:=(*f).([]byte);ok{
		var data = to.Interface()
		json.Unmarshal(v, &data)
		to.Set(reflect.ValueOf(data))
	}else{
		var message = fmt.Sprintf("%v is't a usable bool", f)
		return errors.New(message)
	}
	return nil
}

// 为防止出现没有分配空间的情况，这里提供指针和传值两种
func fetchJsonPtr(f *interface{}, to *reflect.Value)error{
	if *f == nil {
		to.Set(makeNil(*to))
	}
	if v, ok:=(*f).([]byte);ok{
		var data = reflect.New(to.Type().Elem()).Interface()
		json.Unmarshal(v, &data)
		to.Set(reflect.ValueOf(data))
	}else{
		var message = fmt.Sprintf("%v is't a usable bool", f)
		return errors.New(message)
	}
	return nil
}

func makeNil(origin reflect.Value) reflect.Value{
	var typ = origin.Type()
	if typ.Kind() != reflect.Ptr {
		typ = reflect.PtrTo(typ)
	}
	return reflect.Zero(typ)
}
