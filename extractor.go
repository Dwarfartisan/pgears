// extractor.go 中提供了提取字段值的逻辑，主要封装了对 JSON 字段的转换
package pgears

import (
	"encoding/json"
	"reflect"
	_"fmt"
)

// 这个是动态选择的封装接口。除了内置类型的extract，它还提供了对json类型的提取。
func ExtractField(val reflect.Value, field reflect.StructField) interface{} {
	itf := val.Interface()
	if field.Tag.Get("jsonto") != "" {
		j, err := json.Marshal(itf)
		if err != nil {
			panic(err)
		}
		return j
	}
	return itf
}
