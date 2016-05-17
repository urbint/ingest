package convert

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// ToString converts an interface to a string
func ToString(val interface{}) string {
	switch asVal := val.(type) {
	case string:
		return asVal
	case nil:
		return ""
	default:
		value := reflect.ValueOf(val)
		if value.Type().Kind() == reflect.Ptr && value.IsNil() {
			return ""
		}
		return fmt.Sprintf("%v", asVal)
	}
}

// ToInt converts an interface to an int
func ToInt(val interface{}) int {
	switch typedVal := val.(type) {
	case int:
		return typedVal
	case string:
		result, _ := strconv.Atoi(preDecimal(toParsable(typedVal)))
		return result
	default:
		return 0
	}
}

// ToBool converts an interface to a bool
//
// For a string value it will evaluate "true" as true and "false" as false
//
// For any non-bool interface, it will evaluate to whether the value is the zero value if not otherwise specified above
func ToBool(val interface{}) bool {
	switch asVal := val.(type) {
	case bool:
		return asVal
	case string:
		if asVal == "false" {
			return false
		} else if asVal == "true" {
			return true
		}
	}

	return !isZeroVal(val)
}

// ToFloat32 converts an interface to a float32
func ToFloat32(val interface{}) float32 {
	switch asVal := val.(type) {
	case float32:
		return asVal
	case float64:
		return float32(asVal)
	case int:
		return float32(asVal)
	case int32:
		return float32(asVal)
	case int64:
		return float32(asVal)
	case string:
		result, _ := strconv.ParseFloat(toParsable(asVal), 32)
		return float32(result)
	}
	return float32(0)
}

// ToFloat64 converts an interface to a float64
func ToFloat64(val interface{}) float64 {
	switch asVal := val.(type) {
	case float64:
		return asVal
	case float32:
		return float64(asVal)
	case int:
		return float64(asVal)
	case int32:
		return float64(asVal)
	case int64:
		return float64(asVal)
	case string:
		result, _ := strconv.ParseFloat(toParsable(asVal), 64)
		return result
	}
	return float64(0)
}

// ToTime converts a val to a time.
//
// If val is a string, it will be parsed with an optional formatStr argument
// If no formatStr is specified, RFC3339 will be used by default
//
// If val is a int, it will be used as a unix epoc
func ToTime(val interface{}, formatStr ...string) time.Time {
	switch asVal := val.(type) {
	case string:
		layout := time.RFC3339
		if len(formatStr) > 0 && formatStr[0] != "" {
			layout = formatStr[0]
		}
		result, _ := time.Parse(layout, asVal)
		return result
	case int:
		return time.Unix(int64(asVal), 0)
	case int64:
		return time.Unix(asVal, 0)
	case int32:
		return time.Unix(int64(asVal), 0)
	}
	return time.Time{}
}

func unwrapPtr(val reflect.Value) reflect.Value {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return val
}

func preDecimal(str string) string {
	return strings.Split(str, ".")[0]
}

func toParsable(str string) string {
	str = strings.Replace(str, " ", "", -1)
	str = strings.Replace(str, ",", "", -1)
	return str
}

func isZeroVal(val interface{}) bool {
	value := unwrapPtr(reflect.ValueOf(val))
	zeroVal := reflect.Zero(value.Type())

	return reflect.DeepEqual(value.Interface(), zeroVal.Interface())
}
