// exp 包中是用于 SQL 语句生成的各种组件
//
// exp.go 中保存了 exp 包的一些未分类组件
//
// TODO
//
// 普通的表达式没有处理字符串转义，需要接受外部传入的数据的话，请一定参数化
//
// 目前还没有对纯文本文法中的命名做映射，要享受转换的能力，请使用 table 和 field 类型
package exp

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
)

var argFinder, _ = regexp.Compile("([^\\$]\\$\\d)|(^\\$d)")
var dollar, _ = regexp.Compile("\\$\\$")

type Env interface {
	// Type Name to Table Name
	// NOTE: 需要注意的是当前使用type的 Name()，其中包含 packages 名
	TynaToTana(typename string) string
	// Struct Field Name to Table Column Name
	FinaToCona(typename string, fieldname string) string
	Scope() Exp
	SetScope(Exp)
}

func As(exp Exp, name string) Exp {
	return BinOpt("as", exp, Snippet(fmt.Sprintf("\"%s\"", name)))
}

type Exp interface {
	Eval(Env) string
}

type equal struct {
	x, y Exp
}

func Equal(x, y Exp) Exp {
	return &equal{x, y}
}
func (e equal) Eval(env Env) string {
	return fmt.Sprintf("%s=%s", e.x.Eval(env), e.y.Eval(env))
}

type notequal struct {
	x, y Exp
}

func NotEqual(x, y Exp) Exp {
	return &notequal{x, y}
}
func (e notequal) Eval(env Env) string {
	return fmt.Sprintf("%s!=%s", e.x.Eval(env), e.y.Eval(env))
}

type great struct {
	x, y Exp
}

func Great(x, y Exp) Exp {
	return &great{x, y}
}
func (e great) Eval(env Env) string {
	return fmt.Sprintf("%s>%s", e.x.Eval(env), e.y.Eval(env))
}

type less struct {
	x, y Exp
}

func Less(x, y Exp) Exp {
	return &less{x, y}
}
func (e less) Eval(env Env) string {
	return fmt.Sprintf("%s<%s", e.x.Eval(env), e.y.Eval(env))
}

type and struct {
	x, y Exp
}

func And(x, y Exp) Exp {
	return &and{x, y}
}
func (a and) Eval(env Env) string {
	return fmt.Sprintf("(%s) and (%s)", a.x.Eval(env), a.y.Eval(env))
}

type or struct {
	x, y Exp
}

func Or(x, y Exp) Exp {
	return &or{x, y}
}
func (o or) Eval(env Env) string {
	return fmt.Sprintf("(%s) or (%s)", o.x.Eval(env), o.y.Eval(env))
}

type in struct {
	test Exp
	set  []Exp
}

func In(test Exp, set ...Exp) Exp {
	return &in{test, set}
}
func (i in) Eval(env Env) string {
	them := make([]string, 0, len(i.set))
	for _, element := range i.set {
		them = append(them, element.Eval(env))
	}
	set := strings.Join(them, ", ")
	return fmt.Sprintf("%s in (%s)", i.test.Eval(env), set)
}

type text struct {
	data string
}

// Text 函数用于生成text类型的表达式。
// text 就是 PostgreSQL 的 text 类型。由于作者太懒，先用这个代替所有的文本和字符串类型用吧，
// 在 PG 里其实一般的规模好像也问题不大……
func Text(data string) Exp {
	return &text{data}
}

// 从 text 对象到 SQL 片段的解析函数实现
func (t *text) Eval(env Env) string {
	return fmt.Sprintf("'%s'", t.data)
}

type integer struct {
	data int
}

// Integer 函数生成 integer 类型表达式，这个 integer 指 PostgreSQL 中的 integer 类型
func Integer(data int) Exp {
	return &integer{data}
}

// 从 integer 对象到 SQL 片段的解析函数实现
func (i *integer) Eval(env Env) string {
	return fmt.Sprintf("%d", i.data)
}

type timestamp struct {
	data time.Time
}

// Timestamp 函数用于生成 timestamp 类型的表达式。
// timestamp 类型即 PostgreSQL 的 timestamp 时间戳。由于作者太懒，目前还没有 Date
// 和 Datetime 类型的支持。
func Timestamp(data time.Time) Exp {
	return &timestamp{data}
}

// 从 时间戳对象到 SQL 片段的解析函数实现
func (ts *timestamp) Eval(env Env) string {
	dbtime := pq.NullTime{Time: ts.data, Valid: true}
	val, err := dbtime.Value()
	if err != nil {
		panic(err)
	} else {
		if ret, ok := val.([]byte); ok {
			return string(ret)
		} else {
			panic(ts.data)
		}
	}
}

// 之所以用 arg 而不是 parameter ， 完全是为了少写几个字母……
type arg struct {
	Order int
}

// 我也觉得自动生成序列号比较省力气啊。不过想了半天， $%d 就是为了可以指定参数插入位置啊，
// 自动生成顺序就白瞎了啊……
// 将来可能会把这个变成支持命名的，order虽然更省事儿，但是命名相对来说更省心
func Arg(order int) *arg {
	return &arg{order}
}
func (a arg) Eval(env Env) string {
	return fmt.Sprintf("$%d", a.Order)
}

// IncOrder 其实在 pgears 之外应该不太有机会用到，这个是内部生成表达式的时候偶尔
// 需要调整表达式中参数的 order，这个就是起这种作用的，其实如果全命名化了也就没这个问题了。
// 之所以公开是因为 Engine 会用到。
func IncOrder(e Exp, step int) {
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

type function struct {
	name string
	args []Exp
}

func Func(name string, args ...Exp) Exp {
	return &function{name, args}
}
func (f *function) Eval(env Env) string {
	var args = make([]string, len(f.args))
	for idx, arg := range f.args {
		args[idx] = arg.Eval(env)
	}
	return fmt.Sprintf("%s(%s)", f.name, strings.Join(args, ","))
}

type binOpt struct {
	name  string
	left  Exp
	right Exp
}

func BinOpt(name string, left, right Exp) Exp {
	return &binOpt{name, left, right}
}
func (opt *binOpt) Eval(env Env) string {
	return fmt.Sprintf("%s %s %s", opt.left.Eval(env), opt.name,
		opt.right.Eval(env))
}

type not struct {
	exp Exp
}

func Not(exp Exp) *not {
	return &not{exp}
}
func (n not) Eval(env Env) string {
	return fmt.Sprintf("not (%s)", n.exp.Eval(env))
}

type fieldIsNull struct {
	exp Exp
}

func FieldIsNull(field Exp) fieldIsNull {
	return fieldIsNull{field}
}
func (isnull *fieldIsNull) Eval(env Env) string {
	return fmt.Sprintf("%s is NULL", isnull.exp.Eval(env))
}

type isNullFunc struct {
	field Exp
	exp   Exp
}

func IsNullFunc(field Exp, exp Exp) Exp {
	return isNullFunc{field, exp}
}
func (isnull isNullFunc) Eval(env Env) string {
	return fmt.Sprintf("isNull(%s, %s)", isnull.field.Eval(env),
		isnull.exp.Eval(env))
}

type nullif struct {
	field Exp
	exp   Exp
}

func NullIf(field Exp, exp Exp) Exp {
	return nullif{field, exp}
}
func (nullif nullif) Eval(env Env) string {
	return fmt.Sprintf("nullif(%s, %s)", nullif.field.Eval(env),
		nullif.exp.Eval(env))
}

type fullTextSearch struct {
	field Exp
	query Exp
}

func FTS(field Exp, query Exp) *fullTextSearch {
	return &fullTextSearch{field, query}
}
func (fts fullTextSearch) Eval(env Env) string {
	return fmt.Sprintf("%s @@ %s", fts.field.Eval(env),
		fts.query.Eval(env))
}

// snippet 就是直接插入一段纯文本咯，不做任何检测咯，玩脱了是你活该咯……
type snippet struct {
	str *string
}

func Snippet(s string) *snippet {
	return &snippet{&s}
}
func (s *snippet) Eval(env Env) string {
	return *s.str
}

// 虽然理论上说这个内涵的 field 肯定会是field，但是这里姑且还是假定为 exp.Exp
type desc struct {
	field Exp
}

// Desc 生成一个用于orderby desc的表达式
func Desc(field Exp) Exp {
	return &desc{field}
}

func (d desc) Eval(env Env) string {
	return d.field.Eval(env) + " desc"
}

type count struct {
	fields []Exp
	isAll  bool
}

// TODO: postgresql 是不是允许count多个字段列啊……
func Count(fields ...Exp) Exp {
	return &count{fields, false}
}

// count(*)
func Counts() Exp {
	return &count{[]Exp{}, true}
}
func (d count) Eval(env Env) string {
	if d.isAll {
		return "count(*)"
	} else {
		fs := make([]string, 0, len(d.fields))
		for _, f := range d.fields {
			fs = append(fs, f.Eval(env))
		}
		fields := strings.Join(fs, ",")
		return fmt.Sprintf("count(%s)", fields)
	}
}

type brackets struct {
	exp Exp
}

func Brackets(exp Exp) Exp {
	return &brackets{exp}
}
func (b brackets) Eval(env Env) string {
	return fmt.Sprintf("(%s)", b.exp.Eval(env))
}

type allfield struct {
}

func X() Exp {
	return allfield{}
}

func (x allfield) Eval(env Env) string {
	return "*"
}
