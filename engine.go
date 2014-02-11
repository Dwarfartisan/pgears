package pgears

import (
	"fmt"
	"reflect"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/lib/pq"
	"github.com/Dwarfartisan/pgears/exp"
	"errors"
)

type Parser struct {
	*Engine
	scope exp.Exp
}
func NewParser(engine *Engine)(*Parser) {
	return &Parser{engine, nil}
}
func (p *Parser)Scope() exp.Exp {
	return p.scope
}
func (p *Parser)SetScope(exp exp.Exp){
	p.scope = exp
}

// 我原想把所有已经 Prepare过的stmt缓存下来，但是接口还没想清楚，
// 这样是否经济也不确定，先实现一个缓存结构体反射结果的吧，这部分行为
// 已经基本能确认了。接下来再研究数据库层的优化。
type Engine struct {
	*sql.DB
 	//table map to go type
	tablemap map[string]*dbtable
	gomap map[reflect.Type]*dbtable
	gonmap map[string]*dbtable
}
func CreateEngine(url string) (*Engine, error){
	connstring, err := pq.ParseURL(url)
	if err != nil{
		return nil, err
	}
	conn, err := sql.Open("postgres", connstring)
	if err != nil{
		return nil, err
	}
	return &Engine{conn, make(map[string]*dbtable), 
		make(map[reflect.Type]*dbtable),
		make(map[string]*dbtable),
	}, nil
}
//我们可以预先注册一个类型，然后使用这个接口构造与之对应的查询，当我们调用最终
//结果集的FetchOne，会在内部调用对应的merge
//LoadOne 对应 load
func (e *Engine)PrepareFor(typeName string, exp exp.Exp)(*Query, error){
	if table,ok := e.gonmap[typeName]; ok{
		var parser = NewParser(e)
		var sql = exp.Eval(parser)
		var stmt, err = e.DB.Prepare(sql)
		if err != nil {
			return nil, err
		}
		return &Query{stmt, table}, nil
	} else {
		message := typeName + " not found"
		panic(message)
	}
}
// 将类型映射到明确指定的表，遵循一个简单的规则：
// - tag 可以指定类型，不过一般不用，int/int64 对应 integer，
// double 对应 float64, text 对应 string。
// - 如果 int/int64 的字段，tag 指定了 serial， 就是 serial
// - time.Time 类型对应到 timestamp，其实其它时间日期类型也可以，pq支持就可以
// - tag 包含 PK:"true" 的是主键，可以有复合主键，无关类型
// - tag 包含 jsonto:"map" 的 映射到 map[string]interface{}
// - tag 包含 jsonto:"struct" 的映射到结构，具体的结构类型是一个 reflect.Type,
// 保存在 dbfield 类型的 gotype 字段
// - 如果字段定义为值类型，表示对应的是 not null
// - 如果定义为指针类型，表示对应的是可以为null的字段，读取后的使用应该谨慎
// - tag 中的 field:"xxxx" 指定了对应的数据库子段名，这个不能省，一定要写。
// 没做自动转换真的不是因为懒……相信我……
func (e *Engine)MapStructTo(s interface{}, tablename string){
	var val = reflect.ValueOf(s)
	var typ = val.Type().Elem()
	var table = NewDbTable(&typ, tablename)
	e.tablemap[tablename] = table
	e.gomap[typ] = table
	var fullname = fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name())
	e.gonmap[fullname] = table
}

// 将类型注册到指定的表上，这个操作不要求类型完全匹配表结构，只要部分符合，主键完整即可
// 适用于部分内容的填充、更新等操作
// 走这个接口注册类型表示它不是完整的对应数据库中的表。所以可以用表里的数据填充结构，但是
// 不能反过来依赖其中的结构去推断和维护表
// 这个接口显然可以将类型注册到不存在的表，这超出了我最初的设计。建议使用这种表名的时候，
// 起一个跟业务有关的，容易记忆的名字
func (e *Engine)RegistStruct(s interface{}, tablename string){
	var val = reflect.ValueOf(s)
	var typ = val.Type().Elem()
	var table = NewDbTable(&typ, tablename)
	e.gomap[typ] = table
	var fullname = fullGoName(typ)
	e.gonmap[fullname] = table
}
// Type Name to Table Name
// 暂时不支持schema
// NOTE: 需要注意的是当前使用type的Name()，其中包含packages名
func (e *Engine)TynaToTana(typename string) string{
	if dbt,ok := e.gonmap[typename]; ok {
		return dbt.tablename
	} else {
		var message = typename + " not found in registed go types"
		panic(message)
	}
}
// Struct Field Name to Table Column Name
func (e *Engine)FinaToCona(typename string, fieldname string) string{
	if dbt,ok := e.gonmap[typename]; ok {
		if field, ok := (*dbt).fields.GoGet(fieldname); ok {
			return field.DbName
		} else {
			var message = fmt.Sprintf("field %s has't been found in table %s", 
				fieldname, dbt.tablename)
		}
	} else {
		var message = fmt.Sprintf("type %s has't been found in regist", typename)
		panic(message)
	}
}
// 这里要验证传入的obj的类型是否已经注册，但是应该允许匿名类型，这个接口要另外设计
// 目前操作匿名类型可以先拼接一个 Exp ，然后让Engine 去 prepare 出对应的 Query，
// 然后用 Query 和 Result 操作
func (e *Engine)Fetch(obj interface{}) error {
	var typ  = reflect.TypeOf(obj).Elem()
	if m, ok := e.gomap[typ];ok {
		var tabl, pk, fs, cond = m.Extract()
		var sel = exp.Select(fs...).From(tabl).Where(cond)
		var parser = NewParser(e)
		var sql = sel.Eval(parser)
		stmt,err := e.Prepare(sql)
		if err!= nil {
			return err
		}
		var args = make([]interface{}, 0)
		// 因为要填充，无论如何这里也要传入一个指针，不是指针的请自觉panic……
		var val = reflect.ValueOf(obj).Elem()
		for _,p := range pk {
			if pf,ok := p.(*exp.Field); ok{
				var field, _ = typ.FieldByName(pf.GoName)
				var arg interface{} = ExtractField(val.FieldByName(pf.GoName), field)
				args = append(args, arg)
			}
		}
		rset,err := stmt.Query(args...)
		if err != nil {
			return err
		}
		if rset.Next() {
			m.merge(rset, obj)
		}
		return nil
	}else{
		var message = fmt.Sprintf("%v.%v is't a regiested type", 
			typ.PkgPath(), typ.Name())
		return errors.New(message)
	}
}
// insert 的设定是 insert 插入所有字段，包括主键，有时候我们需要在应用层生成主键值，就使用这个逻辑
func (e *Engine)Insert(obj interface{}) error {
	var typ = reflect.TypeOf(obj).Elem()
	if m, ok := e.gomap[typ];ok{
		var tabl, pk, fs, _ = m.Extract()
		for _, p := range pk{
			fs = append(fs, p)
		}
		var ins = exp.Insert(tabl, fs...)
		var parser = NewParser(e)
		var sql = ins.Eval(parser)
		var stmt, err = e.Prepare(sql)
		if err != nil{
			fmt.Println(err)
			return err
		}
		var l = len(pk)
		var args = make([]interface{}, 0, l)
		// 因为要填充，无论如何这里也要传入一个指针，不是指针的请自觉panic……
		var val = reflect.ValueOf(obj).Elem()
		for _,f := range fs {
			if f, ok := f.(*exp.Field);ok {
				var field, _ = typ.FieldByName(f.GoName)
				var arg interface{} = ExtractField(val.FieldByName(f.GoName), field)
				args = append(args, arg)
			}
		}
		_, err = stmt.Query(args...)
		if err != nil {
			return err
		}
		// 因为是完全从应用层取数据，也就不存在对返回结果集的处理，但是这里其实应该校验操作行数
		return nil
	}else{
		var message = fmt.Sprintf("%v.%v is't a regiested type", 
			fullGoName(typ))
		return errors.New(message)
	}
}
// insert merge 的设定是insert仅插入非主键数据，所有主键从数据库加载load后的
// 这个逻辑用于那些主键在数据库层生成的场合，例如自增 id
func (e *Engine)InsertMerge(obj interface{}) error {
	var typ = reflect.TypeOf(obj).Elem()
	if m, ok := e.gomap[typ];ok{
		var tabl, pk, fs, _ = m.Extract()
		var ins = exp.Insert(tabl, fs...).Returning(pk...)
		var parser = NewParser(e)
		var sql = ins.Eval(parser)
		var stmt, err = e.Prepare(sql)
		if err != nil{
			fmt.Println(err)
			return err
		}
		var l = len(pk)
		var args = make([]interface{}, 0, l)
		// 因为要填充，无论如何这里也要传入一个指针，不是指针的请自觉panic……
		var val = reflect.ValueOf(obj).Elem()
		for _,f := range fs {
			if f, ok := f.(*exp.Field);ok {
				var field, _ = typ.FieldByName(f.GoName)
				var arg interface{} = ExtractField(val.FieldByName(f.GoName), field)
				args = append(args, arg)
			}
		}
		rset,err := stmt.Query(args...)
		if err != nil {
			return err
		}
		if rset.Next() {
			m.returning(rset, obj)
		}
		return nil
	}else{
		var message = fmt.Sprintf("%v.%v is't a regiested type", 
			fullGoName(typ))
		return errors.New(message)
	}
}
// update 当前的设定是直接更新，所以无返回，但是——
// TODO:如果返回的受影响数据不为一，记一个warning ，发一个error
func (e *Engine)Update(obj interface{}) error {
	var typ = reflect.TypeOf(obj).Elem()
	if m, ok := e.gomap[typ];ok{
		// 因为要填充，无论如何这里也要传入一个指针，不是指针的请自觉panic……
		var val = reflect.ValueOf(obj).Elem()
		var typ = val.Type()
		var tabl, pk, fs, cond = m.Extract()
		var args = make([]interface{}, 0, len(pk)+len(fs))
		var set = make([]exp.Exp, 0, len(fs))
		var step = len(fs)
		exp.IncOrder(cond, step)
		for idx, f := range fs {
			set = append(set, exp.Equal(f, exp.Arg(idx+1)))
			if fs,ok := f.(*exp.Field); ok {
				var field, _ = typ.FieldByName(fs.GoName)
				var arg interface{} = ExtractField(val.FieldByName(fs.GoName), field)
				args = append(args, arg)
			}
		}
		for _,p := range pk {
			if pf,ok := p.(*exp.Field); ok {
				var field, _ = typ.FieldByName(pf.GoName)
				var arg interface{} = ExtractField(val.FieldByName(pf.GoName), field)
				args = append(args, arg)
			}
		}
		var upd = exp.Update(tabl).Set(set...).Where(cond)
		var parser = NewParser(e)
		var sql = upd.Eval(parser)
		var stmt, err = e.Prepare(sql)
		if err != nil {
			return err
		}
		stmt.Exec(args...)
	}else{
		var message = fmt.Sprintf("%v.%v is't a regiested type", 
			fullGoName(typ))
		return errors.New(message)
	}
	return nil
}
// Dele 当前的设定是根据pk删除，所以无返回，但是——
// TODO:如果返回的受影响数据为0，记一个warning ，发一个error
// 如果大于1，应该log一个Fail，发一个error，必要的话panic也是可以的……
func (e *Engine)Delete(obj interface{}) error {
	var typ = reflect.TypeOf(obj).Elem()
	if m, ok := e.gomap[typ];ok{
		// 因为要填充，无论如何这里也要传入一个指针，不是指针的请自觉panic……
		var val = reflect.ValueOf(obj).Elem()
		var tabl, pk, _, cond = m.Extract()
		var del = exp.Delete(tabl).Where(cond)
		var parser = NewParser(e)
		var sql = del.Eval(parser)
		var stmt, err = e.Prepare(sql)
		if err != nil {
			return err
		}
		var args = make([]interface{}, 0, len(pk))
		for _,p := range pk {
			if pf,ok := p.(*exp.Field); ok {
				var field, _ = typ.FieldByName(pf.GoName)
				var arg interface{} = ExtractField(val.FieldByName(pf.GoName), field)
				args = append(args, arg)
			}
		}
		stmt.Query(args...)
	}else{
		var message = fmt.Sprintf("%v.%v is't a regiested type", 
			fullGoName(typ))
		return errors.New(message)
	}
	return nil
}
// 用于类似 select count(*) from table where cond 这种只需要获取单个结果的查询
// 程序逻辑直接获取单行的第一列，如果查询实际返回的结果集格式不匹配……大概会出错……吧……
func (engine *Engine)Scalar(expr exp.Exp, args... interface{}) (interface{}, error) {
	var parser = NewParser(engine)
	var sql = expr.Eval(parser)
	var row = engine.QueryRow(sql, args...)
	var data interface{}
	var err = row.Scan(&data)
	return data, err
}

type Query struct {
	*sql.Stmt
	table *dbtable
}
func (q *Query)Q(args... interface{}) (*ResultSet, error){
	var rows, err = q.Query(args...)
	if err == nil {
		return &ResultSet{rows, q.table}, nil
	} else {
		return nil, err
	}
}
// 如果有一个已经准备好的 struct ，可以用这个方法传入，会
// 根据反射得到的 accessable 字段拆解出参数传入
// 暂时只是根据顺序提取字段，将来有可能会增加根据字段名和参数名的对照进行传递的功能
func (q *Query)QBy(arg interface{}) (*ResultSet, error){
	var val = reflect.ValueOf(arg)	
	var typ = val.Type()
	var args = make([]interface{}, 0, val.NumField())
	for i:=0;i<val.NumField();i++{
		var field = val.Field(i)
		if field.CanSet() {
			var _arg = ExtractField(field, typ.Field(i))
			args = append(args, &_arg)
		}
	}
	var rows, err = q.Query(args...)
	if err == nil {
		return &ResultSet{rows, q.table}, nil
	} else {
		return nil, err
	}
}

type ResultSet struct{
	*sql.Rows
	table *dbtable
}
//Scan a row and fetch into the object
//严格来说，这里传入的对象应该严格匹配prepare时使用的类型，
//但是从理论来讲，似乎任何结构相同的都可以。有待测试
//此处返回值应为error，但是fetcher构造的时候没有加入，这个将来应该补全
func (r *ResultSet)FetchOne(obj interface{}){
	r.table.merge(r.Rows, obj)
}
func (r *ResultSet)LoadOne(obj interface{}){
	r.table.load(r.Rows, obj)
}
// get the first column in current row, like scalar method
// in .net clr's ado.net
func (r *ResultSet)Scalar(slot interface{}) error {
	cols, err := r.Columns()
	if err != nil {
		return err
	}
	var l = len(cols)
	var slots = make([]interface{}, 0, l)
	slots = append(slots, slot)
	for i:=0; i<l; i++ {
		var slt interface{}
		slots = append(slots, &slt)
	}
	if r.Next() {
		r.Scan(slots...)
		return nil
	}else {
		return errors.New("EOF")
	}
}

