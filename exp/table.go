// table 和 field 都是指应用层命名，也就是表和字段对应的类型名和字段名
// 暂时还不支持主子表的嵌套表示，这个应该尽快实现
// 在 Postgres 中，用双引号包围的命名大小写敏感，可以包含带有空格等特殊符号的字符
// 这里暂时没有计划支持。这个别名功能主要供应用层编程使用，
// 暂时看不到复杂的数据库或应用层命名能带来什么好处。
// NOTE:Table 的 name 应该是模型的类型名而非表名，表名只在注册模型的时候显式出现
package exp
import (
	"fmt"
	"reflect"
	"strings"
)

type Table struct{
	GoName string
	DbName string
	AliasName string
}
// 这里的goname必须是一个fullname，即 type.PkgPath()+"."+type.Name()，
// 如 time.Time。
// 原本想传一个 reflect.Type ，后来我琢磨了一下，在实际调用场景里好像现构造一个
// reflect.Type 也不省心……
func NewTable(goname string) *Table{
	return &Table{goname, "", ""}
}
// 就是类似这样，需要多一行代码，先 var typ = reflect.TypeOf(你手头的那个对象)
// Engine 对象调用已注册对象的curd的时候就需要这个
func TableBy(typ reflect.Type) *Table{
	return &Table{fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name()), "", ""}
}
// 直接指定类型名和表名。一般用不到这个方法，主要是给engine调用的
func TableAs(goname, dbname string) *Table{
	return &Table{goname, dbname, ""}
}
func (t *Table)As(alias string) *Table{
	t.AliasName = alias
	return t
}
func (t *Table)Eval(env Env)string{
	if t.DbName == "" {
		t.DbName = env.TynaToTana(t.GoName)
	}

	if t.AliasName == "" {
		return t.DbName
	} else {
		return fmt.Sprintf("%s as %s", t.DbName, t.AliasName)
	}
}

func (t *Table)Alias()string{
	if t.AliasName == ""{
		return t.DbName
	} else {
		return t.AliasName
	}
}
func (t *Table)Field(f string)*Field{
	return &Field{t, f, ""}
}
func (t *Table)Fields(fields string)[]Exp{
	var fs = strings.Split(fields, ",")
	var ret = make([]Exp, 0, len(fs))
	for _, field := range fs {
		ret = append(ret, t.Field(strings.TrimSpace(field)))
	}
	return ret
}

type Field struct{
	Table *Table
	GoName string
	DbName string
}
func (f *Field)Eval(env Env)string{
	if (f.Table.DbName=="")&&(f.Table.AliasName=="") {
		f.Table.Eval(env)
	}
	if f.DbName=="" {
		f.DbName = env.FinaToCona(f.Table.GoName, f.GoName)
	}
	var scope = env.Scope()
	if _, ok := scope.(Sel); ok {
		return fmt.Sprintf("%s.%s", f.Table.Alias(), f.DbName)
	} else {
		return f.DbName
	}
}
