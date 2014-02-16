package pgears

import (
	"errors"
	"fmt"
	"reflect"
	"github.com/Dwarfartisan/pgears/exp"
	"database/sql"
)

type fetchFunc func(*interface{}, *reflect.Value) error
// stype 指结构字段的类型，这个类型总是指的值类型，如果该字段为指针，就取其 Elem()
// 判断原始类型是否是指针，看它是不是 NotNull 就可以了。
type dbfield struct {
	GoName string
	GoType *reflect.Type
	DbName string
	DbType string
	IsPK bool
	NotNull bool
	Fetch fetchFunc
}
func NewDbField(fieldStruct *reflect.StructField) *dbfield{
	var ret = dbfield{}
	var ftype = fieldStruct.Type
	ret.GoName = fieldStruct.Name
	if ftype.Kind() == reflect.Ptr {
		ret.NotNull = false
		var ft = ftype.Elem()
		ret.GoType = &ft
	} else {
		ret.NotNull = true
		ret.GoType = &ftype
	}
	var tag = fieldStruct.Tag
	ret.DbName = tag.Get("field")
	if pk := tag.Get("pk"); pk=="true" {
		ret.IsPK = true
	}
	ret.Fetch = selectFetch(ret.NotNull, ret.GoType, tag)
	var typ = *ret.GoType
	switch typ.Kind(){
	case reflect.Int, reflect.Int64:
		ret.DbType = "INTEGER"
	case reflect.Float64:
		ret.DbType = "DOUBLE"
	case reflect.String:
		ret.DbType = "TEXT"
	default:
		if tag.Get("jsonto")!=""{
			ret.DbType= "JSON"
		} else if fullGoName(typ)=="time.Time"{
			ret.DbType="TIMESTAMP"
		} 
	}
	return &ret
}
// 根据字段类型选择对应的 fetch 函数
func selectFetch(notnull bool, fieldType *reflect.Type, tag reflect.StructTag) fetchFunc{
	var ft = *fieldType
	switch ft.Kind(){
	case reflect.Bool:
		if notnull {
			return fetchBool
		} else {
			return fetchBoolPtr
		}
	case reflect.Int:
		if notnull {
			return fetchInt
		} else {
				return fetchIntPtr
		}
	case reflect.Int64:
		if notnull {
			return fetchInt64
		} else {
			return fetchInt64Ptr
		}
	case reflect.Float64:
		if notnull {
			return fetchFloat64
		} else {
			return fetchFloat64Ptr
		}
	case reflect.String:
		if notnull {
			return fetchString
		} else {
			return fetchStringPtr
		}
	case reflect.Map:
		if tag.Get("jsonto")=="map" {
			if notnull {
				return fetchJsonMap
			}else{
				return fetchJsonMapPtr
			}
		}
	case reflect.Slice:
		if tag.Get("jsonto")=="bytes" {
			if notnull {
				return fetchByteSlice
			}else{
				return fetchByteSlicePtr
			}
		}
	case reflect.Struct:
		if tag.Get("jsonto")=="struct" {
			if notnull {
				return fetchJsonStruct
			}else{
				return fetchJsonStructPtr
			}
		} 
		if (fullGoName(ft)=="time.Time") {
			if notnull {
				return fetchTime
			} else {
				return fetchTimePtr
			}
		}
	default:
		var message = fmt.Sprintf("I don't know how to fetch it: %v", ft.Kind())
		var err = errors.New(message)
		panic(err)
	}
	return nil
}

// 用于管理字段组的双键map，这样就可以根据结构或表字段名找到对应的字段
type fieldmap struct {
	gomap map[string]*dbfield
	dbmap map[string]*dbfield
}
func NewFieldMap() *fieldmap {
	return &fieldmap{make(map[string]*dbfield), make(map[string]*dbfield)}
}
func (fm *fieldmap)Length() int {
	return len(fm.gomap)
}
func (fm *fieldmap)Set(field *dbfield){
	fm.gomap[field.GoName] = field
	fm.dbmap[field.DbName] = field
}
func (fm *fieldmap)GoGet(goname string) (*dbfield, bool) {
	if field, ok := fm.gomap[goname]; ok {
		return field, ok
	} else {
		return nil, ok
	}
}
func (fm *fieldmap)DbGet(goname string) (*dbfield, bool) {
	if field, ok := fm.dbmap[goname]; ok {
		return field, ok
	} else {
		return nil, ok
	}
}
func (fm *fieldmap)GoKeys() []string {
	var ret = make([]string, 0, len(fm.gomap))
	for key := range fm.gomap {
		ret = append(ret, key)
	}
	return ret
}
func (fm *fieldmap)DbKeys() []string {
	var ret = make([]string, 0, len(fm.dbmap))
	for key := range fm.dbmap {
		ret = append(ret, key)
	}
	return ret
}

type structFetchFunc func(row *sql.Rows, obj interface{})
type dbtable struct {
	tablename string
	gotype *reflect.Type
	fields *fieldmap
	pk *fieldmap
	// load 从数据库中加载所有字段的数据，merge 仅加载非主键字段，用于 Select From Where 类的对象加载
	// returning 仅加载主键字段，用于 insert into returning 类的对象存储
	merge structFetchFunc
	returning structFetchFunc
	load structFetchFunc
}
func NewDbTable(typ *reflect.Type, tablename string) *dbtable {
	var table = dbtable{tablename, typ, NewFieldMap(),
		NewFieldMap(), nil, nil, nil}
	for i:=0;i<(*typ).NumField();i++{
		var field = (*typ).Field(i)
		var df = NewDbField(&field)
		table.fields.Set(df)
		if df.IsPK {
			table.pk.Set(df)
		}
	}
	table.makeLoads()
	return &table
}

// dbtable 是已经解析过的结构体和数据表的定义对照表，所以从中可以生成表、主键和（非主键）数据字段
// 的列表以及用于 where 的 筛选条件（即所有
func (dbt *dbtable)Extract()(t *exp.Table, pk []exp.Exp, other []exp.Exp, cond exp.Exp) {
	t = exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	pk = make([]exp.Exp, 0, dbt.pk.Length())
	other = make([]exp.Exp, 0, dbt.fields.Length())
	for _, key := range dbt.fields.GoKeys() {
		// 这里要取不是pk的
		dbf, _:=dbt.fields.GoGet(key)
		var f = exp.Field{t, dbf.GoName, dbf.DbName}
		if dbf.IsPK {
			pk = append(pk, &f)
		} else {
			other = append(other, &f)
		}
	}
	var gokeys = dbt.pk.GoKeys()
	var f,_ = dbt.pk.GoGet(gokeys[0])
	cond = exp.Equal(t.Field(f.GoName), exp.Arg(1))
	if len(gokeys) > 1 {
		for idx, key := range gokeys[1:] {
			var _f, _ = dbt.pk.GoGet(key)
			cond = exp.And(exp.Equal(t.Field(_f.GoName), exp.Arg(idx+2)),
				cond)
		}
	}
	return t, pk, other, cond
}

// 下面这个内部方法用于构造类似 json/Unmarshal 方法的加载逻辑
// 因为golang还没有泛型，所以如果滥用这些方法，传错类型导致panic，请自挂东南枝(￣^￣)ゞ
// 这个方法本身不执行加载，而是生成加载函数的变量，这样有两个好处，一个是可以套强类型的壳
// 一个是可以把一些确定的逻辑尽可能的优化
func (dbt *dbtable)makeLoads(){
	var fields = make(map[string]*dbfield)
	var pks = make(map[string]*dbfield)
	var data = make(map[string]*dbfield)
	var keys = dbt.fields.DbKeys()
	for _, key := range keys {
		var field, ok = dbt.fields.DbGet(key)
		if !ok {
			continue
		}
		if field.IsPK {
			pks[key] = field
		} else {
			data[key] = field
		}

		fields[key]=field
	}
	dbt.merge = makeFetchHelper(data)
	dbt.returning = makeFetchHelper(pks)
	dbt.load = makeFetchHelper(fields)
}
func makeFetchHelper(fieldmap map[string]*dbfield) structFetchFunc {
	var l = len(fieldmap)
	var slots = make([]interface{}, 0, l)
	for i:=0; i<l; i++ {
		var slot interface{}
		slots = append(slots, &slot)
	}
	var refunc = func(rows *sql.Rows, obj interface{}){
		var val = reflect.Indirect(reflect.ValueOf(obj))
		rows.Scan(slots...)
		var cols, _ = rows.Columns()
		for idx, key := range cols {
			if fdef, ok := fieldmap[key]; ok {
				var fname = fdef.GoName
				if ptr, ok := slots[idx].(*interface{}); ok {
					var field = val.FieldByName(fname)
					fdef.Fetch(ptr, &field)
				}
			}
		}
	}
	return refunc
}

func fullGoName(typ reflect.Type) string {
	return fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name())
}
