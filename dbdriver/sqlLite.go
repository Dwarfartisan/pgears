package dbdriver
/*
	sqlite 驱动单元，封装了pgears的数据库中特殊处理部分
*/
import(
	"database/sql"
	"reflect"
	_ "github.com/mattn/go-sqlite3"
	"errors"
)


func SqliteConnection(url string) (*sql.DB, error){
	conn, err := sql.Open("sqlite3_custom",url)
	if err != nil {
		return nil ,err
	}
	return conn ,err
}

/**
确认tag的dbfieldType是否正确
*/
func CheckType(GoType *reflect.Type , GoName string,RealType string) (string,bool){
	str := getSqlite3DbFieldTypeName(GoType,GoName)
	if str != RealType{
		return "",false
	}
	return str,true
}


/*通过反射实体字段类型翻译成数据库字段类型*/
func getSqlite3DbFieldTypeName(Gotype *reflect.Type,GoName string) string{

	if fldTyp,ok := (*Gotype).FieldByName(GoName) ; ok{
		if fldTyp.Type.Name() == "Time" {
			return ("timestamp")
		}

		kd := fldTyp.Type.Kind()

		switch kd{
		default :
			break
		case reflect.Invalid:
			break
		case reflect.Int,reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64 ,reflect.Uintptr,reflect.Bool :{
		 	return ("integer")
		 	}
		 case reflect.String ,reflect.Interface, reflect.Map,reflect.Ptr:{
		 	return ("text")
			}
		 case reflect.Float32,reflect.Float64,reflect.Complex64,reflect.Complex128:{
		 	return ("float")
			}
		
		}
		
	}
	panic(errors.New("this field is not in Type") )
}



