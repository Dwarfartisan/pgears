package exp

import (
	"fmt"
)

type where struct {
	exp Exp
}
func whereExp(exp Exp)*where{
	return &where{exp}
}
func (w *where)Eval(env Env)string{
	return fmt.Sprintf("where %s", w.exp.Eval(env))
}
