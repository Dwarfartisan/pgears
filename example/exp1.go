package main

import (
	"github.com/Dwarfartisan/pgears"
	"time"
	"fmt"
)


type Account struct {
	Uid   string                  `field:"u_id"      fieldtype:"string" pk:"true"`
	Uname    string                  `field:"u_name"        fieldtype:"string"`
	Lastdate  time.Time             `field:"u_lastdate"    fieldtype:"time"`
}

var Engine *pgears.Engine


func main() {

	Engine, _ = pgears.CreateEngine("sqlite://./test.db")

	var account Account

	Engine.MapStructTo(&account, "auth_account")


	Engine.DropTable("main.Account")
	Engine.CreateTable("main.Account")
	


	var accountm Account

	accountm.Uid = "s12312312sdfsdfsdf"
	accountm.Uname = "ttch"
	accountm.Lastdate = time.Now()

	err := Engine.Insert(&accountm)
	if err != nil {
		panic(fmt.Sprintln("Insert err : %s" ,err))
	}


}