package ingest

import "fmt"
import "reflect"

// A Streamer is used to manipulate input in and out of channels and slices
type Streamer struct {
	Opts StreamOpts
	Log  Logger
	In   <-chan interface{}
	Out  chan interface{}
}

// StreamOpts are the options used to configure a Streamer
type StreamOpts struct {
	Progress chan struct{}
}

// Stream builds a new Streamer that will read from the input channel
func Stream(input <-chan interface{}) *Streamer {
	return &Streamer{
		Log: DefaultLogger.WithField("task", "stream"),
		In:  input,
	}
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

	return &Streamer{
		Log: DefaultLogger.WithField("task", "stream-array"),
		In:  input,
	}
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

// Start starts running the Stream task under the control of the specified controller
func (s *Streamer) Start(ctrl *Controller) <-chan interface{} {
	ctrl = ctrl.Child()
	defer ctrl.ChildBuilt()

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
// The result is untyped as an interface allowing for direct type conversion
func (s *Streamer) Collect(ctrl *Controller) (interface{}, error) {
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

// ForEach runs the specified function on each record in the channel
// If an error is returned, it will be reported to the controller
func (s *Streamer) ForEach(func(interface{}) error) *Streamer {
	panic("Not implemented")
}

// reportProgress will emit a progress event if there is a configured listener
func (s *Streamer) reportProgress() {
	if s.Opts.Progress != nil {
		go func() {
			s.Opts.Progress <- struct{}{}
		}()
	}
}
