package ingest

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

// reportProgress will emit a progress event if there is a configured listener
func (s *Streamer) reportProgress() {
	if s.Opts.Progress != nil {
		go func() {
			s.Opts.Progress <- struct{}{}
		}()
	}
}
