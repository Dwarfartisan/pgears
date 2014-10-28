package exp

import (
	"fmt"
	"strings"
)

type Upd struct {
	tabl  *Table
	set   []Exp
	where Exp
}

func Update(t *Table) *Upd {
	return &Upd{t, nil, nil}
}

// 一开始我想用map，但是想起来现在参数是按顺序传递的，如果用字典会有问题
// 这里用[]*Exp，请务必传入 equal 而不是别的……拜托了……
func (upd *Upd) Set(set ...Exp) *Upd {
	upd.set = set
	return upd
}
func (upd *Upd) Where(exp Exp) *Upd {
	upd.where = exp
	return upd
}
func (upd *Upd) Eval(env Env) string {
	var scope = env.Scope()
	env.SetScope(upd)
	defer env.SetScope(scope)
	var command = fmt.Sprintf("UPDATE %s ", upd.tabl.Eval(env))
	// 虽说要check nil但是如果没有set你update个啥啊……
	// TODO: 此处应当有 panic
	if upd.set != nil {
		command += "SET "
		var sets = make([]string, 0, len(upd.set))
		for _, s := range upd.set {
			sets = append(sets, s.Eval(env))
		}
		command += strings.Join(sets, ", ")
	}
	if upd.where != nil {
		command += " WHERE "
		command += upd.where.Eval(env)
	}
	return command
}
