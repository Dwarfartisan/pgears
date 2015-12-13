package dbdriver

/*
	postgres 驱动单元，封装了pgears的数据库中特殊处理部分
*/


import(
	"reflect"
	"database/sql"
	"errors"
	
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

func GetDbFieldTypeName(Gotype *reflect.Type,GoName string) string{

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