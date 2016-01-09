package dbdriver

/*
	postgres 驱动单元，封装了pgears的数据库中特殊处理部分
*/


import(
	"database/sql"
	
	"github.com/lib/pq"

)


func PostpresConnection(url string) (*sql.DB, error){
	connstring, err := pq.ParseURL(url)
	if err != nil {
		return nil, err
	}
	conn, err := sql.Open("postgres", connstring)
	if err != nil {
		return nil, err
	}
	return conn,nil
}
