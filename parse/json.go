package parse

import (
	"encoding/json"
	"github.com/mcuadros/go-defaults"
	"github.com/urbint/ingest"
	"io"
	"reflect"
	"strings"
)

// A JSONParser handles parsing JSON
type JSONParser struct {
	Opts JSONParseOpts
	Log  ingest.Logger

	In  <-chan io.ReadCloser
	Out chan interface{}

	newRec func() interface{}
}

// JSONParseOpts is used to configure a JSONParser
type JSONParseOpts struct {
	Selection    string
	AbortOnError bool
	NumWorkers   int `default:"1"`
	Progress     chan struct{}
}

// NewJSONParser builds a JSONParser. You will usually want to use parse.JSON instead
func NewJSONParser() *JSONParser {
	parser := &JSONParser{
		Log: ingest.DefaultLogger.WithField("task", "parse-json"),
	}

	defaults.SetDefaults(&parser.Opts)
	return parser
}

// JSON builds a JSONParser which will read from the specified input channel
func JSON(input <-chan io.ReadCloser) *JSONParser {
	parser := NewJSONParser()
	parser.In = input

	return parser
}

// Select sets the Selection that will be used to parse the specified JSON
func (j *JSONParser) Select(selection string) *JSONParser {
	j.Opts.Selection = selection
	return j
}

// AbortOnError is a chainable configuration method that sets whether
// the parser will abort decoding on errors
func (j *JSONParser) AbortOnError(abort bool) *JSONParser {
	j.Opts.AbortOnError = abort
	return j
}

// ReportProgressTo is a chainable configuration method that sets where
// progress will be reported to
func (j *JSONParser) ReportProgressTo(dest chan struct{}) *JSONParser {
	j.Opts.Progress = dest
	return j
}

// WriteTo sets the destination channel for the decoder
// to unmarshal records into
func (j *JSONParser) WriteTo(out chan interface{}) *JSONParser {
	j.Out = out
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
	if j.newRec == nil {
		panic("No known instantiating function. Configure the parser using .Struct")
	}

	childCtrl := ctrl.Child()
	defer childCtrl.ChildBuilt()

	// If we don't have an output channel, make one and close it after we read all the records
	if j.Out == nil {
		j.Out = make(chan interface{})
		go func() {
			childCtrl.Wait()
			close(j.Out)
		}()
	}

	for i := 0; i < j.Opts.NumWorkers; i++ {
		j.startDecodeWorker(childCtrl)
	}

	return j.Out
}

func (j *JSONParser) startDecodeWorker(ctrl *ingest.Controller) {
	ctrl.WorkerStart()
	j.Log.Debug("Starting worker")
	go func() {
		defer ctrl.WorkerEnd()
		defer j.Log.Debug("Exiting worker")
	WorkerAvailable:
		for {
			select {
			case <-ctrl.Quit:
				return
			case reader, ok := <-j.In:
				if !ok {
					return
				}
				done, errs := j.Decode(reader, ctrl.Quit)
				for {
					select {
					case <-done:
						continue WorkerAvailable
					case err := <-errs:
						if j.Opts.AbortOnError {
							ctrl.Err <- err
							return
						}

						log := j.Log.WithError(err)
						switch err := err.(type) {
						case *json.UnmarshalTypeError:
							log = log.WithField("offset", err.Offset).WithField("value", err.Value)
						case *json.SyntaxError:
							log = log.WithField("offset", err.Offset)
						}
						log.Warn("Error unmarshalling JSON record")
					}
				}
			}
		}
	}()
}

// Decode reads an io.Reader into the output channel. It will report errors on the specified error channel.
func (j *JSONParser) Decode(reader io.Reader, abort chan struct{}) (done chan struct{}, errs chan error) {
	done = make(chan struct{})
	errs = make(chan error)
	go func() {
		defer func() { close(done) }()

		decoder := json.NewDecoder(reader)
		if err := j.navigateToSelection(decoder); err != nil {
			errs <- err
			return
		}

		for {
			select {
			case <-abort:
				return
			default:
				if !decoder.More() {
					return
				}
				rec := j.newRec()
				if err := decoder.Decode(rec); err != nil {
					errs <- err
					if j.Opts.AbortOnError {
						return
					}
				}
				select {
				case <-abort:
					return
				case j.Out <- rec:
					j.reportProgress()
					continue
				}
			}
		}
	}()
	return done, errs
}

func (j *JSONParser) navigateToSelection(decoder *json.Decoder) error {
	nestIn := strings.Split(j.Opts.Selection, ".")
	for len(nestIn) > 0 {
		if nestIn[0] == "" {
			nestIn = nestIn[1:]
			continue
		}
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

func (j *JSONParser) reportProgress() {
	if j.Opts.Progress != nil {
		go func() {
			j.Opts.Progress <- struct{}{}
		}()
	}
}
