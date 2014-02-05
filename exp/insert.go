package exp

import (
	"strings"
)

// Insert 结构体的 Returing 操作会返回到结构参数的对效应字段，而 Insert 一组值则会将得到的结果集返回
type Ins struct{
	into *Table
	fields []Exp
	values []Exp
	returning []Exp
}

func Insert(table *Table, fields... Exp) *Ins{
	return &Ins{table, fields, make([]Exp, 0, len(fields)), nil}
}
func (ins *Ins)Values(args... Exp) *Ins{
	for _, a := range args{
		ins.values = append(ins.values, a)
	}
	return ins
}
func (ins *Ins)Returning(fields... Exp) *Ins{
	if ins.returning == nil{
		ins.returning = fields
	} else {
		for _, f := range fields{
			ins.returning = append(ins.returning, f)
		}
	}
	return ins
}
func (ins *Ins)Eval(env Env) string{
	var scope = env.Scope()
	env.SetScope(ins)
	defer env.SetScope(scope)
	var sql = "INSERT INTO "
	sql += ins.into.Eval(env)
	sql += "("
	var fields = make([]string, 0, len(ins.fields))
	for _, f := range ins.fields {
		var field = f.Eval(env)
		fields = append(fields, field)
	}
	sql += strings.Join(fields, ", ")
	sql += ") values("
	if len(ins.values)==0 {
		for i:=0;i<len(ins.fields);i++{
			ins.values = append(ins.values, Arg(i+1))
		}
	}
	var values = make([]string, 0, len(ins.values))
	for _, v := range ins.values {
		values = append(values, v.Eval(env))
	}
	sql += strings.Join(values, ", ")
	sql += ")"
	if len(ins.returning) > 0 {
		var res = make([]string, 0, len(ins.returning))
		for _, ref := range ins.returning {
			res = append(res, ref.Eval(env))
		}
		sql += " returning "
		sql += strings.Join(res, ", ")
	}

	return sql
}

