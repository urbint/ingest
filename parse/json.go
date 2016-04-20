package parse

import (
	"encoding/json"
	"github.com/urbint/ingest"
	"io"
	"reflect"
	"strings"
)

// A JSONParser handles parsing JSON
type JSONParser struct {
	Opts JSONParseOpts
	Log  ingest.Logger

	input <-chan io.ReadCloser

	output    chan interface{}
	ownOutput bool

	newRec func() interface{}
}

// JSONParseOpts is used to configure a JSONParser
type JSONParseOpts struct {
	Selection string
}

// NewJSONParser builds a JSONParser. You will usually want to use parse.JSON instead
func NewJSONParser() *JSONParser {
	return &JSONParser{
		Log: ingest.DefaultLogger.WithField("task", "parse-json"),
	}
}

// JSON builds a JSONParser which will read from the specified input channel
func JSON(input <-chan io.ReadCloser) *JSONParser {
	parser := NewJSONParser()
	parser.input = input

	return parser
}

// Select sets the Selection that will be used to parse the specified JSON
func (j *JSONParser) Select(selection string) *JSONParser {
	j.Opts.Selection = selection
	return j
}

// ReadInto sets the destination channel for the decoder
// to unmarshal records into
func (j *JSONParser) ReadInto(out chan interface{}) *JSONParser {
	j.output = out
	j.ownOutput = false
	return j
}

// Struct sets the type that will be used to allocate new records
func (j *JSONParser) Struct(rec interface{}) *JSONParser {
	indirectType := reflect.Indirect(reflect.ValueOf(rec)).Type()
	j.newRec = func() interface{} {
		return reflect.New(indirectType).Interface()
	}
	return j
}

// Start starts running the parser under the control of the specified controller
func (j *JSONParser) Start(ctrl *ingest.Controller) <-chan interface{} {
	return j.output
}

func (j *JSONParser) navigateToSelection(decoder *json.Decoder) error {
	nestIn := strings.Split(j.Opts.Selection, ".")
	for len(nestIn) > 0 {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if token == nestIn[0] {
			nestIn = nestIn[1:]
			continue
		} else if nestIn[0] == "*" {
			if delimVal, isDelim := token.(json.Delim); isDelim && json.Delim('[') == delimVal {
				nestIn = nestIn[1:]
			}
			continue
		}
	}
	return nil
}
