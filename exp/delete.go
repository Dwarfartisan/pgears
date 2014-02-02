// 
package exp

type Del struct {
	from *Table
	where Exp
}
func Delete(table *Table) *Del{
	return &Del{table, nil}
}
func (del *Del)Where(where Exp) *Del{
	del.where = where
	return del
}
func (del *Del)Eval(env Env)string{
	var sql = "DELETE FROM "
	sql += del.from.Eval(env)
	// 虽然允许生成无where的delete但是还请慎重的使用这样的语句呀
	if del.where != nil {
		sql += " WHERE "
		sql += del.where.Eval(env)
	}
	return sql
}
