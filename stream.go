package ingest

import "fmt"
import "reflect"

// StreamForEachFn is a function that operates over each record in a stream
type StreamForEachFn func(rec interface{}) error

// A Streamer is used to manipulate input in and out of channels and slices
type Streamer struct {
	Opts StreamOpts
	Log  Logger
	In   <-chan interface{}
	Out  chan interface{}

	depGroup *DependencyGroup
	forEachs []StreamForEachFn
}

// StreamOpts are the options used to configure a Streamer
type StreamOpts struct {
	Progress chan struct{}
}

// NewStream builds a new Streamer. Generally you will want to use
// StreamArray or Stream instead
func NewStream() *Streamer {
	return &Streamer{
		depGroup: NewDependencyGroup(),
	}
}

// Stream builds a new Streamer that will read from the input channel
func Stream(input <-chan interface{}) *Streamer {
	stream := NewStream()
	stream.Log = DefaultLogger.WithField("task", "stream")
	stream.In = input
	return stream
}

// StreamArray builds a Streamer which will ready from the provided array
func StreamArray(array interface{}) *Streamer {
	arrValue := reflect.ValueOf(array)

	if arrValue.Kind() != reflect.Slice && arrValue.Kind() != reflect.Array {
		panic(fmt.Sprintf("ingest.StreamArray called on non-array type: %s", arrValue.Type().String()))
	}

	size := arrValue.Len()
	input := make(chan interface{}, size)
	for i := 0; i < size; i++ {
		input <- arrValue.Index(i).Interface()
	}

	stream := NewStream()
	stream.Log = DefaultLogger.WithField("task", "stream-array")
	stream.In = input

	return stream
}

// WriteTo is a chainable configuration method that sets
// where the Streamer will output records to
func (s *Streamer) WriteTo(output chan interface{}) *Streamer {
	s.Out = output
	return s
}

// ReportProgressTo is a chainable configuration method that sets
// where the Streamer will report progress events
func (s *Streamer) ReportProgressTo(progress chan struct{}) *Streamer {
	s.Opts.Progress = progress
	return s
}

// DependOn is a chainable configuration method that will not proceed until all
// specified controllers have resolved
func (s *Streamer) DependOn(ctrls ...*Controller) *Streamer {
	s.depGroup.SetCtrls(ctrls...)
	return s
}

// ForEach is a chainable configuration method that will
// execute a function on the specified stream.
//
// If an error is returned, it will be reported to the controller
// and the record will not be transmitted
//
// For each can be called multiple times, in which case the functions will
// be executed in the order that they were added to the stream
func (s *Streamer) ForEach(fn StreamForEachFn) *Streamer {
	s.forEachs = append(s.forEachs, fn)
	return s
}

// Start starts running the Stream task under the control of the specified controller
func (s *Streamer) Start(ctrl *Controller) <-chan interface{} {
	ctrl = ctrl.Child()
	defer ctrl.ChildBuilt()

	s.depGroup.Wait()

	out := s.Out
	if out == nil {
		out = make(chan interface{})
		go func() {
			ctrl.Wait()
			close(out)
		}()
	}

	ctrl.WorkerStart()
	s.Log.Debug("Starting worker")
	go func() {
		defer ctrl.WorkerEnd()
		defer s.Log.Debug("Exiting worker")
		for {
			select {
			case <-ctrl.Quit:
				return
			case rec, ok := <-s.In:
				if !ok {
					return
				}
				if err := s.runForEachs(rec); err != nil {
					ctrl.Err <- err
					continue
				}
				select {
				case <-ctrl.Quit:
					return
				case out <- rec:
					s.reportProgress()
					continue
				}
			}
		}
	}()

	return out
}

// Collect reads the results of the Input into an array, under the control of the specified controller.
//
// If the Controller is aborted, ErrAborted will be returned
func (s *Streamer) Collect(ctrl *Controller) ([]interface{}, error) {
	resultChan := s.Start(ctrl)

	results := []interface{}{}

	for {
		select {
		case rec, ok := <-resultChan:
			if !ok {
				return results, nil
			}
			results = append(results, rec)
		case err := <-ctrl.Err:
			return nil, err
		case <-ctrl.Quit:
			return nil, ErrAborted
		}
	}
}

// reportProgress will emit a progress event if there is a configured listener
func (s *Streamer) reportProgress() {
	if s.Opts.Progress != nil {
		go func() {
			s.Opts.Progress <- struct{}{}
		}()
	}
}

// runForEachs will run all of the forEach functions on the specified record, or
// return the first error encountered.
//
// If there are no forEachs, it is essentially a no-op
func (s *Streamer) runForEachs(rec interface{}) error {
	if s.forEachs == nil || len(s.forEachs) == 0 {
		return nil
	}

	for _, fn := range s.forEachs {
		if err := fn(rec); err != nil {
			return err
		}
	}

	return nil
}
