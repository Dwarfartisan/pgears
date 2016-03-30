package exp

import "fmt"

type join struct {
	joit *Table
	on   Exp
}

func joinExp(t *Table) *join {
	return &join{t, nil}
}
func (j *join) onExp(on Exp) *join {
	j.on = on
	return j
}
func (e *join) Eval(env Env) string {
	return fmt.Sprintf("join %s on %s", e.joit.Eval(env), e.on.Eval(env))
}

type leftjoin struct {
	leftjoin *Table
	on       Exp
}

func leftjoinExp(t *Table) *leftjoin {
	return &leftjoin{t, nil}
}
func (j *leftjoin) onExp(on Exp) *leftjoin {
	j.on = on
	return j
}
func (e *leftjoin) Eval(env Env) string {
	return fmt.Sprintf("left join %s on %s", e.leftjoin.Eval(env), e.on.Eval(env))
}
