package parse

import (
	"encoding/xml"
	"github.com/mcuadros/go-defaults"
	"github.com/urbint/ingest"
	"io"
	"reflect"
)

// A XMLParser handles parsing XML
type XMLParser struct {
	Opts XMLParseOpts
	Log  ingest.Logger

	In  <-chan io.ReadCloser
	Out chan interface{}

	newRec   func() interface{}
	depGroup *ingest.DependencyGroup
}

// XMLParseOpts is used to configure a XMLParser
type XMLParseOpts struct {
	Selection    string
	AbortOnError bool
	NumWorkers   int `default:"1"`
	Progress     chan struct{}
	Strict       bool `default:"true"`
	Entities     map[string]string
}

// NewXMLParser builds a XMLParser. You will usually want to use parse.XML instead
func NewXMLParser() *XMLParser {
	parser := &XMLParser{
		Log:      ingest.DefaultLogger.WithField("task", "parse-xml"),
		depGroup: ingest.NewDependencyGroup(),
	}

	defaults.SetDefaults(&parser.Opts)
	return parser
}

// XML builds a XMLParser which will read from the specified input channel
func XML(input <-chan io.ReadCloser) *XMLParser {
	parser := NewXMLParser()
	parser.In = input

	return parser
}

// XMLReader reads a single io.ReadCloser
func XMLReader(input io.ReadCloser) *XMLParser {
	parser := NewXMLParser()
	in := make(chan io.ReadCloser, 1)
	in <- input
	close(in)

	parser.In = in

	return parser
}

// Select sets the Selection that will be used to parse the specified XML
func (x *XMLParser) Select(selection string) *XMLParser {
	x.Opts.Selection = selection
	return x
}

// Strict is a chainable configuration method that sets the Strict option for the parser
func (x *XMLParser) Strict(strict bool) *XMLParser {
	x.Opts.Strict = strict
	return x
}

// EntityMap is a chainable configuration method that sets the Entities for the parser
func (x *XMLParser) EntityMap(entities map[string]string) *XMLParser {
	x.Opts.Entities = entities
	return x
}

// AbortOnError is a chainable configuration method that sets whether
// the parser will abort decoding on errors
func (x *XMLParser) AbortOnError(abort bool) *XMLParser {
	x.Opts.AbortOnError = abort
	return x
}

// ReportProgressTo is a chainable configuration method that sets where
// progress will be reported to
func (x *XMLParser) ReportProgressTo(dest chan struct{}) *XMLParser {
	x.Opts.Progress = dest
	return x
}

// DependOn is a chainable configuration method that will not proceed until all
// specified controllers have resolved
func (x *XMLParser) DependOn(ctrls ...*ingest.Controller) *XMLParser {
	x.depGroup.SetCtrls(ctrls...)
	return x
}

// WriteTo sets the destination channel for the decoder
// to unmarshal records into
func (x *XMLParser) WriteTo(out chan interface{}) *XMLParser {
	x.Out = out
	return x
}

// Struct sets the type that will be used to allocate new records
func (x *XMLParser) Struct(rec interface{}) *XMLParser {
	indirectType := reflect.Indirect(reflect.ValueOf(rec)).Type()
	x.newRec = func() interface{} {
		return reflect.New(indirectType).Interface()
	}
	return x
}

// Collect runs the parser syncronously and returns the results as an array, or any error encountered
func (x *XMLParser) Collect(ctrl *ingest.Controller) ([]interface{}, error) {
	return ingest.Stream(x.Start(ctrl)).Collect(ctrl)
}

// Start starts running the parser under the control of the specified controller
func (x *XMLParser) Start(ctrl *ingest.Controller) <-chan interface{} {
	if x.newRec == nil {
		panic("No known instantiating function. Configure the parser using .Struct")
	}

	childCtrl := ctrl.Child()
	defer childCtrl.ChildBuilt()

	x.depGroup.Wait()

	// If we don't have an output channel, make one and close it after we read all the records
	if x.Out == nil {
		x.Out = make(chan interface{})
		go func() {
			childCtrl.Wait()
			close(x.Out)
		}()
	}

	for i := 0; i < x.Opts.NumWorkers; i++ {
		x.startDecodeWorker(childCtrl)
	}

	return x.Out
}

func (x *XMLParser) startDecodeWorker(ctrl *ingest.Controller) {
	ctrl.WorkerStart()
	x.Log.Debug("Starting worker")
	go func() {
		defer ctrl.WorkerEnd()
		defer x.Log.Debug("Exiting worker")
	WorkerAvailable:
		for {
			select {
			case <-ctrl.Quit:
				return
			case reader, ok := <-x.In:
				if !ok {
					return
				}
				done, errs := x.Decode(reader, ctrl.Quit)
				for {
					select {
					case <-done:
						continue WorkerAvailable
					case err := <-errs:
						if x.Opts.AbortOnError {
							ctrl.Err <- err
							return
						}
						x.Log.WithError(err).Warn("Error unmarshalling XML record")
					}
				}
			}
		}
	}()
}

// Decode reads an io.Reader into the output channel. It will report errors on the specified error channel.
func (x *XMLParser) Decode(reader io.Reader, abort chan struct{}) (done chan struct{}, errs chan error) {
	done = make(chan struct{})
	errs = make(chan error)
	if x.Opts.Selection == "" {
		panic("No known token Selection provided. Configure the parser using .Select()")
	}
	go func() {
		defer func() { close(done) }()

		decoder := xml.NewDecoder(reader)
		decoder.Strict = x.Opts.Strict
		decoder.Entity = x.Opts.Entities

		for {
			select {
			case <-abort:
				return
			default:
				token, err := decoder.Token()
				if err == io.EOF {
					return
				} else if err != nil {
					errs <- err
					continue
				}
				rec := x.newRec()
				found := false
				if se, ok := token.(xml.StartElement); ok {
					if se.Name.Local == x.Opts.Selection {
						found = true
						if err := decoder.DecodeElement(rec, &se); err != nil {
							errs <- err
							if x.Opts.AbortOnError {
								return
							}
						}
					}
				}
				if found {
					select {
					case <-abort:
						return
					case x.Out <- rec:
						x.reportProgress()
						continue
					}
				}
			}
		}
	}()
	return done, errs
}

func (x *XMLParser) reportProgress() {
	if x.Opts.Progress != nil {
		go func() {
			x.Opts.Progress <- struct{}{}
		}()
	}
}
