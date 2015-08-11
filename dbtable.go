package pgears

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/Dwarfartisan/pgears/exp"
)

// DbField 结构用 Go 语言表述一个数据库字段的结构。
// stype 指结构字段的类型，这个类型总是指的值类型，如果该字段为指针，就取其 Elem()
// 判断原始类型是否是指针，看它是不是 NotNull 就可以了。
type DbField struct {
	GoName  string
	DbName  string
	IsPK    bool
	DbGen   bool
	NotNull bool
	Extract func(reflect.Value) (interface{}, func() error)
}

// NewDbField 是 DbField 的内部构造函数，通常由其它pgears内部类型调用
func NewDbField(fieldStruct *reflect.StructField) *DbField {
	var ret = DbField{}
	ret.GoName = fieldStruct.Name
	ftype := fieldStruct.Type
	switch ftype.Kind() {
	case reflect.Ptr, reflect.Interface:
		ret.NotNull = false
	default:
		ret.NotNull = true
	}
	var tag = fieldStruct.Tag
	ret.DbName = tag.Get("field")
	if pk := tag.Get("pk"); pk == "true" {
		ret.IsPK = true
	}
	if dbgen := tag.Get("dbgen"); dbgen == "true" {
		ret.DbGen = true
	}
	if tag.Get("jsonto") == "" {
		ret.Extract = func(field reflect.Value) (interface{}, func() error) {
			return field.Addr().Interface(), nil
		}
	} else {
		ret.Extract = func(field reflect.Value) (interface{}, func() error) {
			var buffer []byte
			return &buffer, func() error {
				slot := field.Addr().Interface()
				var err error
				if buffer != nil {
					err = json.Unmarshal(buffer, &slot)
				}
				return err
			}
		}
	}
	return &ret
}

// FieldMap 结构用于管理字段组的双键 map，这样就可以根据结构或表字段名找到对应的字段
type FieldMap struct {
	gomap map[string]*DbField
	dbmap map[string]*DbField
}

// NewFieldMap 函数构造一个go 结构到 数据表结构的映射关系
func NewFieldMap() *FieldMap {
	return &FieldMap{make(map[string]*DbField), make(map[string]*DbField)}
}

// Length 返回 gomap 的长度，即 go 结构的字段数目
func (fm *FieldMap) Length() int {
	return len(fm.gomap)
}

// Set 方法在映射集中注册一个新的 DbField
func (fm *FieldMap) Set(field *DbField) {
	fm.gomap[field.GoName] = field
	fm.dbmap[field.DbName] = field
}

// GoGet 方法获取 go 结构中给定字段名对应的那个 Dbfield ，如果该成员不存在，状态码返回 false
func (fm *FieldMap) GoGet(goname string) (*DbField, bool) {
	if field, ok := fm.gomap[goname]; ok {
		return field, ok
	}
	return nil, false
}

// DbGet 方法获取数据库结构中给定字段名对应的那个 Dbfield ，如果该成员不存在，状态码返回 false
func (fm *FieldMap) DbGet(goname string) (*DbField, bool) {
	if field, ok := fm.dbmap[goname]; ok {
		return field, ok
	}
	return nil, false
}

// GoKeys 方法给出所有注册的 Go 结构字段名
func (fm *FieldMap) GoKeys() []string {
	var ret = make([]string, 0, len(fm.gomap))
	for key := range fm.gomap {
		ret = append(ret, key)
	}
	return ret
}

// DbKeys 方法给出所有注册的数据库字段名
func (fm *FieldMap) DbKeys() []string {
	var ret = make([]string, 0, len(fm.dbmap))
	for key := range fm.dbmap {
		ret = append(ret, key)
	}
	return ret
}

type structFetchFunc func(row *sql.Rows, obj interface{})

// DbTable 用 Go 语言描述了数据表的结构
type DbTable struct {
	tablename string
	gotype    *reflect.Type
	Fields    *FieldMap
	Pk        *FieldMap
	NPk       *FieldMap
	DbGen     *FieldMap
	NDbGen    *FieldMap
	// all 从数据库中加载所有字段的数据，pk 仅加载主键列表， npk 仅加载非pk字段，用于 Select From Where 类的对象加载
	// dbgens 仅加载dbgen字段，用于 insert into returning 类的对象存储
	pk        structFetchFunc
	npk       structFetchFunc
	returning structFetchFunc
	all       structFetchFunc
}

// NewDbTable 构造一个数据表映射结构
func NewDbTable(typ *reflect.Type, tablename string) *DbTable {
	var table = DbTable{tablename, typ, NewFieldMap(),
		NewFieldMap(), NewFieldMap(), NewFieldMap(), NewFieldMap(),
		nil, nil, nil, nil}
	for i := 0; i < (*typ).NumField(); i++ {
		var field = (*typ).Field(i)
		var df = NewDbField(&field)
		table.Fields.Set(df)
		if df.IsPK {
			table.Pk.Set(df)
		} else {
			table.NPk.Set(df)
		}
		if df.DbGen {
			table.DbGen.Set(df)
		} else {
			table.NDbGen.Set(df)
		}
	}
	table.makeLoads()
	return &table
}

// 有时候调用者要根据具给定业务对象的内容拼接SQL表达式，为了便利，这时候需要提供一个既定的参考列表
// 而非确定的表达式，以下若干Extract和XxxGears方法用于这种场合
// DbTable 是已经解析过的结构体和数据表的定义对照表，所以从中可以生成表、主键和（非主键）数据字段
// 的列表以及用于 where 的 筛选条件（即所有主键的 and 表达式）

// Extract 方从 DbTable 结构中得到分别表示主键、字段表达式和条件表达式的部分，便于拼接
func (dbt *DbTable) Extract() (t *exp.Table, pk []exp.Exp, other []exp.Exp, cond exp.Exp) {
	t = exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	pk = make([]exp.Exp, 0, dbt.Pk.Length())
	other = make([]exp.Exp, 0, dbt.NPk.Length())
	for _, key := range dbt.Fields.GoKeys() {
		// 这里要取不是pk的
		dbf, _ := dbt.Fields.GoGet(key)
		var f = exp.Field{Table: t, GoName: dbf.GoName, DbName: dbf.DbName}
		if dbf.IsPK {
			pk = append(pk, &f)
		} else {
			other = append(other, &f)
		}
	}
	var gokeys = dbt.Pk.GoKeys()
	var f, _ = dbt.Pk.GoGet(gokeys[0])
	cond = exp.Equal(t.Field(f.GoName), exp.Arg(1))
	if len(gokeys) > 1 {
		for idx, key := range gokeys[1:] {
			var _f, _ = dbt.Pk.GoGet(key)
			cond = exp.And(exp.Equal(t.Field(_f.GoName), exp.Arg(idx+2)),
				cond)
		}
	}
	return t, pk, other, cond
}

// MergeInsertGears 方法生成用于 Insert 的表达式组件，其中不包括在数据库端自动生成的字段，
// 这些字段包含在 returning 中
func (dbt *DbTable) MergeInsertGears() (t *exp.Table,
	fields []exp.Exp, values []exp.Exp, returning []exp.Exp,
	names []string) {
	t = exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	fields = make([]exp.Exp, 0, dbt.Fields.Length())
	values = make([]exp.Exp, 0, dbt.Fields.Length())
	returning = make([]exp.Exp, 0, dbt.DbGen.Length())
	names = make([]string, 0, dbt.Fields.Length())
	idx := 0
	for _, key := range dbt.Fields.GoKeys() {
		// 这里要取不是dbgen的
		dbf, _ := dbt.Fields.GoGet(key)
		var f = exp.Field{Table: t, GoName: dbf.GoName, DbName: dbf.DbName}
		if dbf.DbGen {
			returning = append(returning, &f)
		} else {
			fields = append(fields, &f)
			idx++
			values = append(values, exp.Arg(idx))
			names = append(names, dbf.GoName)
		}
	}
	return t, fields, values, returning, names
}

// AllInsertGears 方法生成用于 Insert 的表达式组件，包含所有的字段，包括dbgen
func (dbt *DbTable) AllInsertGears() (t *exp.Table,
	fields []exp.Exp, values []exp.Exp, names []string) {
	t = exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	fields = make([]exp.Exp, 0, dbt.Fields.Length())
	names = make([]string, 0, dbt.Fields.Length())
	values = make([]exp.Exp, 0, len(fields))
	for idx, key := range dbt.Fields.GoKeys() {
		dbf, _ := dbt.Fields.GoGet(key)
		var f = exp.Field{Table: t, GoName: dbf.GoName, DbName: dbf.DbName}
		fields = append(fields, &f)
		arg := exp.Arg(idx + 1)
		values = append(values, arg)
		names = append(names, dbf.GoName)
	}

	return t, fields, values, names
}

// UpdateGears 方法生成用于 Update 的表达式组件，这里需要调用者传入 sets 的字段列表
func (dbt *DbTable) UpdateGears(s []string) (t *exp.Table,
	sets []exp.Exp, cond exp.Exp, names []string) {
	t = exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	pk := dbt.Pk.GoKeys()
	sets = make([]exp.Exp, 0, len(s))
	names = make([]string, 0, len(s)+len(pk))
	for idx, skey := range s {
		sets = append(sets, exp.Equal(t.Field(skey), exp.Arg(idx+1)))
	}
	start := len(s)
	for _, key := range pk {
		names = append(names, key)
	}

	var f, _ = dbt.Pk.GoGet(pk[0])
	cond = exp.Equal(t.Field(f.GoName), exp.Arg(start+1))
	if len(pk) > 1 {
		for idx, key := range pk[1:] {
			var _f, _ = dbt.Pk.GoGet(key)
			cond = exp.And(exp.Equal(t.Field(_f.GoName), exp.Arg(start+idx+2)),
				cond)
		}
	}

	return t, sets, cond, names
}

// 以下若干XxxExpr方法用于生成便于调用的既定表达式
// 表达式内含的参数列，其对应的字段列表在返回值中给出

// FetchExpr 方法生成一个用于 Select 包含所有主键的给定对象的 SQL 表达式
func (dbt *DbTable) FetchExpr() (expr exp.Exp, names []string) {
	t := exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	pk := make([]exp.Exp, 0, dbt.Pk.Length())
	other := make([]exp.Exp, 0, dbt.Fields.Length())
	for _, key := range dbt.Fields.GoKeys() {
		// 这里要取不是pk的
		dbf, _ := dbt.Fields.GoGet(key)
		var f = exp.Field{Table: t, GoName: dbf.GoName, DbName: dbf.DbName}
		if dbf.IsPK {
			pk = append(pk, &f)
		} else {
			other = append(other, &f)
		}
	}
	var gokeys = dbt.Pk.GoKeys()
	var f, _ = dbt.Pk.GoGet(gokeys[0])
	cond := exp.Equal(t.Field(f.GoName), exp.Arg(1))
	if len(gokeys) > 1 {
		for idx, key := range gokeys[1:] {
			var _f, _ = dbt.Pk.GoGet(key)
			cond = exp.And(exp.Equal(t.Field(_f.GoName), exp.Arg(idx+2)),
				cond)
		}
	}
	return exp.Select(other...).Where(cond), gokeys
}

// MergeInsertExpr 方法生成一个用于 Insert 的表达式，其中不包括在数据库端自动生成的字段，这些字段包含在
// returning 中
func (dbt *DbTable) MergeInsertExpr() (exp.Exp, []string) {
	t := exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	dbgen := make([]exp.Exp, 0, dbt.DbGen.Length())
	other := make([]exp.Exp, 0, dbt.Fields.Length())
	args := make([]exp.Exp, 0, dbt.Fields.Length())
	names := make([]string, 0, dbt.Fields.Length())
	idx := 1
	for _, key := range dbt.Fields.GoKeys() {
		// 这里要取不是dbgen的
		dbf, _ := dbt.Fields.GoGet(key)
		var f = exp.Field{Table: t, GoName: dbf.GoName, DbName: dbf.DbName}
		if dbf.DbGen {
			dbgen = append(dbgen, &f)
		} else {
			other = append(other, &f)
			arg := exp.Arg(idx)
			idx++
			args = append(args, arg)
			names = append(names, dbf.GoName)
		}
	}
	return exp.Insert(t, other...).Values(args...).Returning(dbgen...), names
}

// AllInsertExpr 方法生成一个用于 Insert 的表达式，包含所有的字段，包括dbgen
func (dbt *DbTable) AllInsertExpr() (exp.Exp, []string) {
	t := exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	fields := make([]exp.Exp, 0, dbt.Fields.Length())
	args := make([]exp.Exp, 0, dbt.Fields.Length())
	names := dbt.Fields.GoKeys()
	for idx, key := range names {
		dbf, _ := dbt.Fields.GoGet(key)
		var f = exp.Field{Table: t, GoName: dbf.GoName, DbName: dbf.DbName}
		fields = append(fields, &f)
		arg := exp.Arg(idx + 1)
		args = append(args, arg)
	}
	return exp.Insert(t, fields...).Values(args...), names
}

// UpdateExpr 方法生成一个用于 Update 的表达式，这里需要调用者给出准备Update的字段名，
// 函数生成形如 Update XXX Set ... Where cond 的 SQL 表达式，
// update 语句中包含主键字段列表，所以虽然它的sets由用户指定，仍然返回参数命名表
func (dbt *DbTable) UpdateExpr(sets []string) (expr exp.Exp, names []string) {
	t := exp.TableAs(fullGoName(*dbt.gotype), dbt.tablename)
	copy(names, sets)
	pk := make([]exp.Exp, 0, dbt.Pk.Length())
	setExprs := make([]exp.Exp, 0, len(sets))
	for idx, key := range sets {
		arg := exp.Arg(idx + 1)
		setExprs = append(setExprs, exp.Equal(t.Field(key), arg))
	}

	start := len(sets)
	var gokeys = dbt.Pk.GoKeys()
	for _, key := range gokeys {
		dbf, _ := dbt.Fields.GoGet(key)
		var f = exp.Field{Table: t, GoName: dbf.GoName, DbName: dbf.DbName}
		pk = append(pk, &f)
		names = append(names, key)
	}

	var f, _ = dbt.Pk.GoGet(gokeys[0])
	cond := exp.Equal(t.Field(f.GoName), exp.Arg(start))
	if len(gokeys) > 1 {
		for idx, key := range gokeys[1:] {
			var _f, _ = dbt.Pk.GoGet(key)
			cond = exp.And(exp.Equal(t.Field(_f.GoName), exp.Arg(start+idx+2)),
				cond)
		}
	}

	return exp.Update(t).Set(setExprs...).Where(cond), names
}

// 下面这个内部方法用于构造类似 json/Unmarshal 方法的加载逻辑
// 因为golang还没有泛型，所以如果滥用这些方法，传错类型导致panic，请自挂东南枝(￣^￣)ゞ
// 这个方法本身不执行加载，而是生成加载函数的变量，这样有两个好处，一个是可以套强类型的壳
// 一个是可以把一些确定的逻辑尽可能的优化
func (dbt *DbTable) makeLoads() {
	var fields = make(map[string]*DbField)
	var pks = make(map[string]*DbField)
	var npk = make(map[string]*DbField)
	var dbgen = make(map[string]*DbField)
	var keys = dbt.Fields.DbKeys()
	for _, key := range keys {
		var field, ok = dbt.Fields.DbGet(key)
		if !ok {
			continue
		}
		if field.IsPK {
			pks[key] = field
		} else {
			npk[key] = field
		}
		if field.DbGen {
			dbgen[key] = field
		}

		fields[key] = field
	}
	dbt.pk = makeFetchHelper(pks)
	dbt.npk = makeFetchHelper(npk)
	dbt.returning = makeFetchHelper(dbgen)
	dbt.all = makeFetchHelper(fields)
}
func makeFetchHelper(FieldMap map[string]*DbField) structFetchFunc {

	var refunc = func(rows *sql.Rows, obj interface{}) {
		var cols, err = rows.Columns()
		if err != nil {
			panic(err)
		}
		l := len(cols)
		var val = reflect.Indirect(reflect.ValueOf(obj))
		var slots = make([]interface{}, l)
		var callbacks = make([]func() error, 0, l)
		for idx, col := range cols {
			if dbf, ok := FieldMap[col]; ok {
				var fname = dbf.GoName
				field := val.FieldByName(fname)
				slot, callback := dbf.Extract(field)
				slots[idx] = slot
				if callback != nil {
					callbacks = append(callbacks, callback)
				}
			}
		}
		rows.Scan(slots...)
		for _, cb := range callbacks {
			err := cb()
			if err != nil {
				panic(err)
			}
		}
	}
	return refunc
}

func fullGoName(typ reflect.Type) string {
	return fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name())
}
