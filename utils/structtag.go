package utils

import (
	"reflect"
	"strings"
)

// MapFromStructTag builds a hash map using the string specified by tagName as the keys
//
// If dest is specified, it will be used as the map where the result is stored. Otherwise a new
// map will be initialized and returned.
//
// Optionally omitempty may be added to the value to cause a zero value of the field to be
// omitted from the map entirely
func MapFromStructTag(src interface{}, tagName string, dest ...map[string]interface{}) map[string]interface{} {
	var result map[string]interface{}

	if len(dest) == 0 {
		result = map[string]interface{}{}
	} else {
		result = dest[0]
	}

	srcValue := reflect.ValueOf(src)
	srcType := reflect.Indirect(srcValue).Type()

	for i := 0; i < srcType.NumField(); i++ {
		srcStructField := srcType.Field(i)
		tagValues := strings.Split(srcStructField.Tag.Get(tagName), ",")

		if tagValues[0] == "-" {
			continue
		}

		outName := tagValues[0]
		if outName == "" {
			outName = srcStructField.Name
		}

		var omitEmpty bool
		if len(tagValues) > 1 && tagValues[1] == "omitempty" {
			omitEmpty = true
		}

		fieldVal := reflect.Indirect(srcValue).Field(i)

		iFieldVal := fieldVal.Interface()

		if omitEmpty && reflect.DeepEqual(reflect.Zero(srcStructField.Type).Interface(), iFieldVal) {
			continue
		} else if srcStructField.Type.Kind() == reflect.Struct {
			if srcStructField.Anonymous {
				MapFromStructTag(fieldVal.Interface(), tagName, result)
			} else {
				result[outName] = MapFromStructTag(fieldVal.Interface(), tagName)
			}
		} else {
			result[outName] = iFieldVal
		}
	}

	return result
}
