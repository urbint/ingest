package ingest

import (
	"sync"
)

// An ImportFn is a function that will be called by an importer when it is run
// it recieves the active ingest controller and is expected to return any
// error encountered during the run
type ImportFn func(ctrl *Controller) error

// An Importer is a control structure around an ingest.Controller that reduces
// the amount of boilerplate required for simple importers.
//
// It implements a foreman.Abortable interface
type Importer struct {
	ctrl *Controller
	fn   ImportFn
	mu   sync.Mutex
}

// NewImporter builds an importer with the speicifed ImportFn
func NewImporter(fn ImportFn) *Importer {
	return &Importer{
		fn: fn,
	}
}

// Run runs the specified importer calling the function specified at construction
func (i *Importer) Run() error {
	ctrl := i.BuildNewController()
	return i.fn(ctrl)
}

// Abort will abort the importer if it is running
func (i *Importer) Abort() {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.ctrl != nil {
		i.ctrl.Abort()
		i.ctrl = nil
	}
}

// BuildNewController allocates a new ingest.Controller for the importer, while
// waiting for the appropriate locks
//
// It returns the controller for convenience
func (i *Importer) BuildNewController() *Controller {
	i.mu.Lock()
	i.ctrl = NewController()
	i.mu.Unlock()

	return i.ctrl
}
