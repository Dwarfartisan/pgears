package exp

import "fmt"

type join struct{
	joit *Table
	on Exp
}
func joinExp(t *Table) *join {
	return &join{t, nil}
}
func (j *join)onExp(on Exp) *join{
	j.on = on
	return j
}
func (e *join)Eval(env Env) string{
	return fmt.Sprintf("join %s on %s", e.joit.Eval(env), e.on.Eval(env))
}
