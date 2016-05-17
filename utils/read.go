package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/ghodss/yaml"
)

// ReadConfig reads the specified config and parses the corresponding
// config into a result map.
//
// Currently supported extensions are JSON and YAML
func ReadConfig(filePath string) (map[string]interface{}, error) {
	ext := path.Ext(filePath)

	contents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var settings map[string]interface{}

	switch ext {
	case ".json":
		if err = json.Unmarshal(contents, &settings); err != nil {
			return nil, err
		}
	case ".yaml":
		fallthrough
	case ".yml":
		if err = yaml.Unmarshal(contents, &settings); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unrecognized extension: %s", ext)
	}

	return settings, nil
}
