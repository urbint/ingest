package convert

import (
	"reflect"
	"time"
)

// MapToStruct reads a map into a destination applying the appropriate conversions as necessary
//
// lookupKey is an optional argument that specifies the struct tag that will be used to read values from
// the map. If no key is specified, the field name will be read directly
//
// time.Times can automatically be parsed. You may specify the format of the date string by using the `format` struct tag
//
// Returns the dest as a convenience
func MapToStruct(src map[string]interface{}, dest interface{}, lookupKey ...string) interface{} {
	destValue := unwrapPtr(reflect.ValueOf(dest))
	destType := destValue.Type()

	var lookupTag string
	if len(lookupKey) > 0 {
		lookupTag = lookupKey[0]
	}

	numFields := destType.NumField()

	for i := 0; i < numFields; i++ {
		structField := destType.Field(i)

		var readKey string
		if lookupTag == "" {
			readKey = structField.Name
		} else {
			readKey = structField.Tag.Get(lookupTag)
		}

		srcVal, hasVal := src[readKey]
		if !hasVal && !structField.Anonymous {
			continue
		} else if structField.Anonymous {
			srcVal = src
		}

		fieldValue := destValue.Field(i)

		switch structField.Type.Kind() {
		case reflect.String:
			fieldValue.SetString(ToString(srcVal))
		case reflect.Bool:
			fieldValue.SetBool(ToBool(srcVal))
		case reflect.Int:
			fieldValue.SetInt(int64(ToInt(srcVal)))
		case reflect.Float32:
			fieldValue.SetFloat(float64(ToFloat32(srcVal)))
		case reflect.Float64:
			fieldValue.SetFloat(ToFloat64(srcVal))
		case reflect.Struct:
			switch fieldValue.Interface().(type) {
			case time.Time:
				parsed := ToTime(srcVal, structField.Tag.Get("format"))
				fieldValue.Set(reflect.ValueOf(parsed))
			default:
				ptr := reflect.New(fieldValue.Type())
				MapToStruct(srcVal.(map[string]interface{}), ptr.Interface(), lookupTag)
				fieldValue.Set(ptr.Elem())
			}
		case reflect.Ptr:
			ptr := reflect.New(fieldValue.Type().Elem())
			MapToStruct(srcVal.(map[string]interface{}), ptr.Interface(), lookupTag)
			fieldValue.Set(ptr)
		}
	}

	return dest
}
