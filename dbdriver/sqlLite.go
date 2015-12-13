package dbdriver
/*
	sqlite 驱动单元，封装了pgears的数据库中特殊处理部分
*/
import(
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)


func SqliteConnection(url string) (*sql.DB, error){
	conn, err := sql.Open("sqlite3",url)
	if err != nil {
		return nil ,err
	}
	return conn ,err
}
