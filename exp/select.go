// select.go 文件提供以 Sel 类型为核心的 Select 语句生成功能
//
// NOTE
//
// Table 的 name 应该是模型的类型名而非表名，表名只在注册模型的时候显式出现
//
package exp

import (
	"fmt"
	"strings"
)

// desc 应该能作用到确定的排序字段
type Sel struct {
	selects []Exp
	from    *Table
	join    []Exp
	where   Exp
	groupby []Exp
	having  Exp
	orderby []Exp
	limit   *int
	offset  *int
}

func SelectThem(fields ...string) *Sel {
	var fs = make([]Exp, 0)
	for _, fname := range fields {
		fs = append(fs, &Field{nil, fname, ""})
	}
	return &Sel{selects: fs,
		from:    nil,
		join:    nil,
		where:   nil,
		groupby: nil,
		having:  nil,
		orderby: nil,
		limit:   nil,
	}
}
func Select(fields ...Exp) *Sel {
	return &Sel{selects: fields,
		from:    nil,
		join:    nil,
		where:   nil,
		groupby: nil,
		having:  nil,
		orderby: nil,
		limit:   nil,
	}
}

func (sel *Sel) From(t *Table) *Sel {
	sel.from = t
	for _, f := range sel.selects {
		if field, ok := f.(*Field); ok {
			if field.Table == nil {
				field.Table = t
			}
		}
	}
	return sel
}
func (sel *Sel) Join(t *Table, on Exp) *Sel {
	if sel.join == nil {
		sel.join = make([]Exp, 0)
	}
	sel.join = append(sel.join, joinExp(t).onExp(on))
	return sel
}
func (sel *Sel) Where(exp Exp) *Sel {
	sel.where = whereExp(exp)
	return sel
}
func (sel *Sel) GroupBy(fields ...Exp) *Sel {
	if sel.groupby == nil {
		sel.groupby = make([]Exp, 0)
	}
	for _, f := range fields {
		sel.groupby = append(sel.groupby, f)
	}
	return sel
}
func (sel *Sel) Having(exp Exp) *Sel {
	sel.having = exp
	return sel
}
func (sel *Sel) OrderBy(fields ...Exp) *Sel {
	if sel.orderby == nil {
		sel.orderby = fields
	} else {
		for _, f := range fields {
			sel.orderby = append(sel.orderby, f)
		}
	}
	return sel
}

// 本来我想把 limit 和 offset 设定成 Exp ，但是想来想去写了这么多SQL从来没有这样的用法咯……
func (sel *Sel) Limit(limit int) *Sel {
	sel.limit = &limit
	return sel
}
func (sel *Sel) Offset(offset int) *Sel {
	sel.offset = &offset
	return sel
}
func (sel Sel) Eval(env Env) string {
	var scope = env.Scope()
	env.SetScope(sel)
	defer env.SetScope(scope)
	var command = "SELECT "
	if sel.selects != nil {
		var fields = make([]string, 0)
		for _, f := range sel.selects {
			var fname = f.Eval(env)
			fields = append(fields, fname)
		}
		command += strings.Join(fields, ", ")
	}
	if sel.from != nil {
		command += (" FROM " + sel.from.Eval(env))
	}
	if sel.join != nil {
		for _, j := range sel.join {
			command += (" " + j.Eval(env))
		}
	}
	if sel.where != nil {
		command += (" " + sel.where.Eval(env))
	}
	if sel.groupby != nil {
		var groups = make([]string, 0)
		for _, g := range sel.groupby {
			groups = append(groups, g.Eval(env))
		}
		command += (" GROUP BY " + strings.Join(groups, ", "))
	}
	if sel.having != nil {
		command += " HAVING " + sel.having.Eval(env)
	}
	if sel.orderby != nil {
		var orderby = make([]string, 0)
		for _, o := range sel.groupby {
			orderby = append(orderby, o.Eval(env))
		}
		command += (" ORDER BY " + strings.Join(orderby, ", "))
	}
	if sel.limit != nil {
		command += fmt.Sprintf(" LIMIT %d", *sel.limit)
	}
	if sel.offset != nil {
		command += fmt.Sprintf(" OFFSET %d", *sel.offset)
	}
	return command
}
