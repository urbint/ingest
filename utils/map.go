package utils

import (
	"fmt"
	"strings"
)

// MapTransform is used to define transformations that will happen on a map
type MapTransform map[string]string

// CopyMap creates a new map with the same key/value pairs as the source map.
//
// An optional MapTransform argument can be passed in which will define a sequence of
// transformations that will occur on the map
func CopyMap(src map[string]interface{}, transforms ...MapTransform) (map[string]interface{}, error) {
	result := map[string]interface{}{}

	for k, v := range src {
		result[k] = v
	}

	if len(transforms) > 0 {
		if err := TransformMap(result, transforms[0]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// TransformMap will apply the specified transformations to the source map
//
// If a transformation is unable to be applied, an error will be returned.
func TransformMap(target map[string]interface{}, transformations MapTransform) error {
	for srcKey, destKey := range transformations {
		srcMap, err := navigateToParent(target, srcKey, false)
		if err != nil {
			return err
		}

		srcKey = lastStep(srcKey)

		if val, hasKey := srcMap[srcKey]; hasKey {
			destMap, err := navigateToParent(target, destKey, true)
			if err != nil {
				return err
			}

			delete(srcMap, srcKey)
			destKey = lastStep(destKey)
			if destKey != "-" {
				destMap[destKey] = val
			}
		}
	}
	return nil
}

func navigateToParent(target map[string]interface{}, path string, createAsNeeded bool) (map[string]interface{}, error) {
	steps := strings.Split(path, ".")

	result := target
	for i := 0; i < len(steps)-1; i++ {
		if _, hasInner := result[steps[i]]; !hasInner {
			if createAsNeeded {
				newMap := map[string]interface{}{}
				result[steps[i]] = newMap
			} else {
				return nil, nil
			}
		}
		var isMap bool
		if result, isMap = (result[steps[i]]).(map[string]interface{}); !isMap {
			return nil, fmt.Errorf(`Attempted to access nested target "%s", but could not find path to "%s"`, path, steps[i])
		}
	}
	return result, nil
}

func lastStep(path string) string {
	steps := strings.Split(path, ".")
	return steps[len(steps)-1]
}
