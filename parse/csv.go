package parse

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mcuadros/go-defaults"
	"github.com/urbint/ingest"
)

// CSVDecodeError is an error encountered while decoding a parsed CSV row into
// an interface
type CSVDecodeError struct {
	SrcErr error
}

func (c *CSVDecodeError) Error() string {
	return fmt.Sprintf("Parse Error: %s", c.SrcErr.Error())
}

// A CSVParser handles parsing CSV
type CSVParser struct {
	Opts CSVParserOpts
	Log  ingest.Logger

	In        <-chan io.ReadCloser
	Out       chan interface{}
	ownOutput bool
	newRec    func() interface{}
}

// CSVParserOpts are used to configure a CSVParser
type CSVParserOpts struct {
	AbortOnError bool
	TrimSpaces   bool   `default:"true"`
	NumWorkers   int    `default:"1"`
	DateFormat   string `default:"01/02/2006"`
	Progress     chan struct{}
}

// NewCSVParser builds a CSVParser. Usually, parse.CSV is preferred
func NewCSVParser() *CSVParser {
	parser := &CSVParser{
		Log: ingest.DefaultLogger.WithField("task", "parse-csv"),
	}

	defaults.SetDefaults(&parser.Opts)
	return parser
}

// CSV builds a CSVParser which will read from the specified input channel
func CSV(input <-chan io.ReadCloser) *CSVParser {
	parser := NewCSVParser()
	parser.In = input
	return parser
}

// AbortOnError is a chainable configuration method that sets whether
// the parser will abort decoding on errors
func (c *CSVParser) AbortOnError(abort bool) *CSVParser {
	c.Opts.AbortOnError = abort
	return c
}

// TrimSpaces is a chainable configuration method that sets whether
// the parser will trim spaces around rows
func (c *CSVParser) TrimSpaces(trim bool) *CSVParser {
	c.Opts.TrimSpaces = trim
	return c
}

// ReportProgressTo is a chainable configuration method that sets where
// progress will be reported to
func (c *CSVParser) ReportProgressTo(dest chan struct{}) *CSVParser {
	c.Opts.Progress = dest
	return c
}

// WriteTo sets the destination channel for the decoder
// to unmarshal records into
func (c *CSVParser) WriteTo(out chan interface{}) *CSVParser {
	c.Out = out
	c.ownOutput = false
	return c
}

// DateFormat is a chainable configuration method used
// to configure the format string that will be used by
// time.Parse to read dates found within the CSV
func (c *CSVParser) DateFormat(fmt string) *CSVParser {
	c.Opts.DateFormat = fmt
	return c
}

// Struct is a chainable configuration method that sets the base struct
// that will be used to allocate new records
func (c *CSVParser) Struct(rec interface{}) *CSVParser {
	indirectType := reflect.Indirect(reflect.ValueOf(rec)).Type()
	c.newRec = func() interface{} {
		return reflect.New(indirectType).Interface()
	}
	return c
}

// Start starts running the parser under the control of the specified controller
func (c *CSVParser) Start(ctrl *ingest.Controller) <-chan interface{} {
	if c.newRec == nil {
		panic("No known instantiating function. Configure the parser using .Struct")
	}

	childCtrl := ctrl.Child()
	defer childCtrl.ChildBuilt()

	// If we own the output channel, close it after all of our workers have exited
	if c.ownOutput {
		go func() {
			childCtrl.Wait()
			close(c.Out)
		}()
	}

	for i := 0; i < c.Opts.NumWorkers; i++ {
		c.startDecodeWorker(childCtrl)
	}

	return c.Out
}

// Decode will read records from a single reader until it has finished or abort is called.
//
// If the Parser is configured to AbortOnError it will quit on a Parse error.
func (c *CSVParser) Decode(input io.ReadCloser, abort chan struct{}) (chan interface{}, chan error) {
	done := make(chan interface{})
	errs := make(chan error)

	go func() {
		defer func() { close(done) }()

		reader := csv.NewReader(input)
		defer input.Close()

		header, err := reader.Read()
		if err != nil {
			errs <- err
			return
		}
		fieldMap := c.parseHeaderForType(header, c.newRec())

		for {
			select {
			case <-abort:
				return
			default:
				row, err := reader.Read()
				if err == io.EOF {
					return
				} else if err != nil {
					errs <- err
					continue
				}

				rec, err := c.parseRowWithFieldMap(row, fieldMap)
				if err != nil {
					errs <- err
				}
				select {
				case <-abort:
					return
				case c.Out <- rec:
					c.reportProgress()
					continue
				}
			}
		}
	}()

	return done, errs
}

func (c *CSVParser) startDecodeWorker(ctrl *ingest.Controller) {
	ctrl.WorkerStart()
	c.Log.Debug("Starting worker")
	go func() {
		defer ctrl.WorkerEnd()
		defer c.Log.Debug("Exiting worker")
	WorkerAvailable:
		for {
			select {
			case <-ctrl.Quit:
				return
			case reader, ok := <-c.In:
				if !ok {
					return
				}
				done, errs := c.Decode(reader, ctrl.Quit)
				for {
					select {
					case <-done:
						continue WorkerAvailable
					case err := <-errs:
						if c.Opts.AbortOnError {
							ctrl.Err <- err
							return
						}
						log := c.Log.WithError(err)
						if parseErr, isParseError := err.(*csv.ParseError); isParseError && parseErr.Err == csv.ErrFieldCount {
							log.Warn("Error parsing CSV Row")
						} else if _, isDecodeErr := err.(*CSVDecodeError); isDecodeErr {
							log.Warn("Error decoding CSV Row")
						} else {
							log.Error("Unknown CSV Error")
							ctrl.Err <- err
							return
						}
					}
				}
			}
		}
	}()
}

// parseHeaderForType builds a header map from a single row using
// the struct tags specified in the mapper.
//
// FieldMap is a map of intergers representing the index of the column of the CSV row mapped
// to the indicies of the field. On the struct(s if embedded).
func (c *CSVParser) parseHeaderForType(header []string, mapper interface{}) map[int][]int {
	targetType := reflect.Indirect(reflect.ValueOf(mapper)).Type()
	result := map[int][]int{}
	for column := 0; column < len(header); column++ {
		trimmed := strings.TrimSpace(header[column])
		findResult, _ := findFieldInStruct(trimmed, targetType)
		result[column] = findResult
	}
	return result
}

// parseRowWithFieldMap reads a single row with the specified field map and returns a newly built record
func (c *CSVParser) parseRowWithFieldMap(row []string, fieldMap map[int][]int) (rec interface{}, err error) {
	rec = c.newRec()
	instance := reflect.ValueOf(rec).Elem()

	for j := 0; j < len(row); j++ {
		fieldIndicies := fieldMap[j]

		// If the length of the string is 0, or we don't have a mapping
		// keep the "nil" version of the struct field
		if len(fieldIndicies) == 0 || len(row[j]) == 0 {
			continue
		}

		field := instance
		for _, fieldIndex := range fieldIndicies {
			field = field.Field(fieldIndex)
		}

		fieldInterface := field.Interface()
		switch fieldInterface.(type) {
		case string:
			if c.Opts.TrimSpaces {
				field.SetString(strings.TrimSpace(row[j]))
			} else {
				field.SetString(row[j])
			}
		case float32:
			val, err := strconv.ParseFloat(row[j], 32)
			if err != nil {
				return nil, &CSVDecodeError{fmt.Errorf("Error parsing float: %v", row[j])}
			}
			field.SetFloat(val)
		case int:
			val, err := strconv.Atoi(row[j])
			if err != nil {
				return nil, &CSVDecodeError{fmt.Errorf("Error parsing int: %v", row[j])}
			}
			field.SetInt(int64(val))
		case int8:
			val, err := strconv.ParseInt(row[j], 10, 8)
			if err != nil {
				return nil, &CSVDecodeError{fmt.Errorf("Error parsing int: %v", row[j])}
			}
			field.SetInt(val)
		case uint8:
			val, err := strconv.ParseUint(row[j], 10, 8)
			if err != nil {
				return nil, &CSVDecodeError{fmt.Errorf("Error parsing uint: %v", row[j])}
			}
			field.SetUint(val)
		case uint16:
			val, err := strconv.ParseUint(row[j], 10, 16)
			if err != nil {
				return nil, &CSVDecodeError{fmt.Errorf("Error parsing uint: %v", row[j])}
			}
			field.SetUint(val)
		case uint32:
			val, err := strconv.ParseUint(row[j], 10, 32)
			if err != nil {
				return nil, &CSVDecodeError{fmt.Errorf("Error parsing uint: %v", row[j])}
			}
			field.SetUint(val)
		case time.Time:
			time, err := time.Parse(c.Opts.DateFormat, row[j])
			if err != nil {
				return nil, &CSVDecodeError{fmt.Errorf("Error parsing date: %v", row[j])}
			}
			field.Set(reflect.ValueOf(time))
		default:
			return nil, &CSVDecodeError{fmt.Errorf("Unhandled type: %v", field.Type().String())}
		}
	}
	return rec, nil
}

func (c *CSVParser) reportProgress() {
	if c.Opts.Progress != nil {
		go func() {
			c.Opts.Progress <- struct{}{}
		}()
	}
}

// findFieldInStruct is used by parseHeader to locate the index of the selected field inside
// of the target.
//
// If the target contains embedded structs, the result array will be the path of indicies to
// the selected field.
func findFieldInStruct(fieldName string, target reflect.Type) (result []int, found bool) {
	numFields := target.NumField()
	for i := 0; i < numFields; i++ {
		field := target.Field(i)
		kind := field.Type.Kind()

		isEmbeddedStruct := kind == reflect.Struct || (kind == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct)

		if isEmbeddedStruct {
			var nestedTarget reflect.Type
			if kind == reflect.Struct {
				nestedTarget = field.Type
			} else {
				nestedTarget = field.Type.Elem()
			}
			nestedIndexes, found := findFieldInStruct(fieldName, nestedTarget)
			if found {
				result := append([]int{i}, nestedIndexes...)
				return result, true
			}
		} else {
			csvName := field.Tag.Get("csv")
			if csvName == fieldName {
				return []int{i}, true
			}
		}
	}
	return []int{}, false
}
