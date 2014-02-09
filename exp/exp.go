// TODO：普通的表达式没有处理字符串转义，需要接受外部传入的数据的话，请一定参数化
// TODO: Stringer 接口应该改为exp接口，exp.evel接受engine参数
// 目前还没有对纯文本文法中的命名做映射，要享受转换的能力，请使用table和field类型
package exp

import (
	"fmt"
	"time"
	"strings"
	"regexp"
)

var argFinder,_ = regexp.Compile("([^\\$]\\$\\d)|(^\\$d)")
var dollar,_ = regexp.Compile("\\$\\$")

type Env interface {
	// Type Name to Table Name
	// NOTE: 需要注意的是当前使用type的Name()，其中包含packages名
	TynaToTana(typename string) string
	// Struct Field Name to Table Column Name
	FinaToCona(typename string, fieldname string) string
	Scope() Exp
	SetScope(Exp)
}

type Exp interface {
	Eval(Env) string
}

type equal struct{
	x, y Exp
}
func Equal(x, y Exp) Exp{
	return &equal{x, y}
}
func (e equal)Eval(env Env)string{
	return fmt.Sprintf("%s=%s", e.x.Eval(env), e.y.Eval(env))
}

type notequal struct{
	x, y Exp
}
func NotEqual(x, y Exp) Exp{
	return &notequal{x, y}
}
func (e notequal)Eval(env Env)string{
	return fmt.Sprintf("%s!=%s", e.x.Eval(env), e.y.Eval(env))
}

type great struct{
	x, y Exp
}
func Great(x, y Exp) Exp{
	return &great{x, y}
}
func (e great)Eval(env Env)string{
	return fmt.Sprintf("%s>%s", e.x.Eval(env), e.y.Eval(env))
}

type less struct{
	x, y Exp
}
func Less(x, y Exp) Exp{
	return &great{x, y}
}
func (e less)Eval(env Env)string{
	return fmt.Sprintf("%s<%s", e.x.Eval(env), e.y.Eval(env))
}

type and struct{
	x, y Exp
}
func And(x, y Exp) Exp{
	return &and{x, y}
}
func (a and)Eval(env Env)string{
	return fmt.Sprintf("(%s) and (%s)", a.x.Eval(env), a.y.Eval(env))
}

type or struct{
	x, y Exp
}
func Or(x, y Exp) Exp{
	return &or{x, y}
}
func (o or)Eval(env Env)string{
	return fmt.Sprintf("(%s) or (%s)", o.x.Eval(env), o.y.Eval(env))
}

// text 就是 PostgreSQL 的 text 类型。由于作者太懒，先用这个代替所有的文本和字符串类型用吧，
// 在 PG 里其实一般的规模好像也问题不大……
type text struct{
	data string
}
func Text(data string) Exp{
	return &text{data}
}
func(t *text)Eval(env Env)string{
	return fmt.Sprintf("'%s'", t.data)
}

type integer struct{
	data int
}
func Integer(data int) Exp{
	return &integer{data}
}
func(i *integer)Eval(env Env)string{
	return fmt.Sprintf("%d", i.data)
}

type timestamp struct{
	data time.Time
}
func TimeStamp(data time.Time) Exp {
	return &timestamp{data}
}
func(ts *timestamp)Eval(env Env) string{
	return fmt.Sprintf("'%v'::Timestamp", ts.data)
}

// 之所以用 arg 而不是 parameter ， 完全是为了少写几个字母……
type arg struct{
	Order int
}
// 我也觉得自动生成序列号比较省力气啊。不过想了半天， $%d 就是为了可以指定参数插入位置啊，
// 自动生成顺序就白瞎了啊……
// 将来可能会把这个变成支持命名的，order虽然更省事儿，但是命名相对来说更省心
func Arg(order int) *arg {
	return &arg{order}
}
func (a arg)Eval(env Env)string{
	return fmt.Sprintf("$%d", a.Order)
}
// 写到 Update 一个对象的时候发现需要一个方法，将引擎中的条件表达式的所有条件参数的
// order 增加一个整数。这种问题真是太暗黑了……
func IncOrder(e Exp, step int){
	switch ex := e.(type) {
	case *equal:
		IncOrder((*ex).x, step)
		IncOrder((*ex).y, step)
	case *notequal:
		IncOrder((*ex).x, step)
		IncOrder((*ex).y, step)
	case *and:
		IncOrder((*ex).x, step)
		IncOrder((*ex).y, step)
	case *or:
		IncOrder((*ex).x, step)
		IncOrder((*ex).y, step)
	case *great:
		IncOrder((*ex).x, step)
		IncOrder((*ex).y, step)
	case *less:
		IncOrder((*ex).x, step)
		IncOrder((*ex).y, step)
	case *not:
		IncOrder((*ex).exp, step)
	case *arg:
		ex.Order += step
	}
}

type function struct{
	name string
	args []Exp
}
func Func(name string, args... Exp) Exp{
	return &function{name, args}
}
func (f *function)Eval(env Env)string{
	var args = make([]string, len(f.args))
	for _, arg := range f.args{
		args = append(args, arg.Eval(env))
	}
	return fmt.Sprintf("%s(\"%s\")", f.name, strings.Join(args, ","))
}


type not struct{
	exp Exp
}
func Not(exp Exp) *not{
	return &not{exp}
}
func (n not)Eval(env Env) string{
	return fmt.Sprintf("not (%s)", n.exp.Eval(env))
}

type fieldIsNull struct{
	exp Exp
}
func FieldIsNull(field Exp) fieldIsNull{
	return fieldIsNull{field}
}
func (isnull *fieldIsNull)Eval(env Env) string{
	return fmt.Sprintf("%s is NULL", isnull.exp.Eval(env))
}

type isNullFunc struct{
	field Exp
	exp Exp
}
func IsNullFunc(field Exp, exp Exp) Exp{
	return isNullFunc{field, exp}
}
func (isnull isNullFunc)Eval(env Env) string{
	return fmt.Sprintf("isNull(%s, %s)", isnull.field.Eval(env),
		isnull.exp.Eval(env))
}

type nullif struct{
	field Exp
	exp Exp
}
func NullIf(field Exp, exp Exp) Exp{
	return nullif{field, exp}
}
func (nullif nullif)Eval(env Env) string{
	return fmt.Sprintf("nullif(%s, %s)", nullif.field.Eval(env),
		nullif.exp.Eval(env))
}

type fullTextSearch struct{
	field Exp
	query Exp
}
func FTS(field Exp, query Exp) *fullTextSearch{
	return &fullTextSearch{field, query}
}
func (fts fullTextSearch)Eval(env Env)string{
	return fmt.Sprintf("%s @@ %s", fts.field.Eval(env),
		fts.query.Eval(env))
}

// snippet 就是直接插入一段纯文本咯，不做任何检测咯，玩脱了是你活该咯……
type snippet struct {
	str *string
}
func Snippet(s string) *snippet{
	return &snippet{&s}
}
func (s *snippet)Eval(env Env) string {
	return *s.str
}

// 虽然理论上说这个内涵的 field 肯定会是field，但是这里姑且还是假定为 exp.Exp
type desc struct {
	field Exp
}
// Desc 生成一个用于orderby desc的表达式
func Desc(field Exp)Exp{
	return &{field}
}

func (f field)Eval(env Env) string{
	return field.Eval(env)+" desc"
}
