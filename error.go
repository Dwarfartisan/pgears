package pgears

import (
	"fmt"
)

type NotFound struct{
	message string
}

func NewNotFound(object interface{}) NotFound{
	var obj interface{}
	if o,ok := obj.(*interface{});ok {
		obj = o
	} else {
		obj = object
	}
	var message = fmt.Sprintf("%v not found", obj)
	return NotFound{message}
}

func (e NotFound)Error()string{
	return e.message
}
