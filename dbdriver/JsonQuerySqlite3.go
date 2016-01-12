package dbdriver

import (
	"database/sql"
	"fmt"
	sqlite "github.com/mattn/go-sqlite3"
	"encoding/json"
	p "github.com/Dwarfartisan/goparsec2"
)

type JsonHelper map[string]interface{};


func JSONP(name, field string)string{

	jh := make(JsonHelper)

	var m = []byte(name)

	json.Unmarshal(m,&jh)

	return fmt.Sprintf("%s",jh[JSONF(field)])
}

func JsonQuerySqlite3(name string) string{

	if name == "#>>"{
		return "JSONP"
	}

	return name;
}


func getName(st p.State) (interface{}, error) {
	return p.Do(func(state p.State) interface{} {
		_,_ = p.Try(p.Chr('{'))(state)
		data := make([]rune, 0, 0)
		for{
			val,_ := p.Try(p.NChr('}'))(state)
			if(val  == nil){
				return string(data)
			}
			data = append(data, val.(rune))
		}
		return nil
	})(st)
}


func JSONF(name string)string{

	st := p.BasicStateFromText(name)

	sname,err := getName(&st)

	if(err != nil){
		panic(err)
	}
	if c ,ok := sname.(string);ok{
		return c;
	}
	panic("error field name")
}


func init(){

	sql.Register("sqlite3_custom", &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {
			if err := conn.RegisterFunc("JSONP", JSONP, true); err != nil {
				return err
			}
			return nil
		},
	})
}