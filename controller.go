package ingest

import (
	"sync"
)

// A Controller ...
type Controller struct {
	// Err is used by workers to report errors encountered while working
	Err chan error
	// Quit is closed by the controller when Abort is called
	Quit chan struct{}

	wg         sync.WaitGroup
	parent     *Controller
	childBuilt bool
	mu         sync.Mutex
}

// NewController builds a new Controller for use
func NewController() *Controller {
	ctrl := &Controller{
		Err:  make(chan error),
		Quit: make(chan struct{}),
		wg:   sync.WaitGroup{},
		mu:   sync.Mutex{},
	}
	return ctrl
}

// ChildBuilt is called on a child Controller to denote that it has been built and its wait group is valid
func (c *Controller) ChildBuilt() *Controller {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.parent == nil {
		panic("ChildBuilt called on a non-child Controller")
	}

	if c.childBuilt {
		return c
	}

	c.childBuilt = true
	c.wg.Done()
	return c
}

// WorkerStart is called by Worker to indicate to the controller
// that it is running and that the job is not complete until it exits
func (c *Controller) WorkerStart() *Controller {
	c.wg.Add(1)
	return c
}

// WorkerEnd is called by a Worker to signal that is has finished
// running
func (c *Controller) WorkerEnd() *Controller {
	c.wg.Done()
	return c
}

// Wait waits for all workers to have exited
func (c *Controller) Wait() {
	c.wg.Wait()
}

// Done returns a channel that will be closed when the worker controller has finished
func (c *Controller) Done() chan struct{} {
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	return done
}

// Error waits for all workers to exit or reports the first error encountered by a worker
//
// If an error is encountered, it will call Quit to terminate all running workers
func (c *Controller) Error() error {
	select {
	case err := <-c.Err:
		close(c.Quit)
		return err
	case <-c.Done():
		return nil
	}
}

// Abort aborts all running workers and waits for them to finish
func (c *Controller) Abort() {
	close(c.Quit)
	c.Wait()
}

// ReportStartTo is syntactical sugar around sending an empty struct to a channel
// in a chainable API
func (c *Controller) ReportStartTo(start chan struct{}) *Controller {
	go func() {
		start <- struct{}{}
	}()
	return c
}

// ReportEndTo will send an event when the Controller finishes
func (c *Controller) ReportEndTo(end chan struct{}) *Controller {
	go func() {
		c.Wait()
		end <- struct{}{}
	}()
	return c
}

// Child creates a new child controller that will quit when the parent quits
// and report errors back to the parent.
//
// It will, however keep independent wait groups and all of its workers
// will only count as a single worker to its parent.
func (c *Controller) Child() *Controller {
	child := NewController()
	child.parent = c
	child.wg.Add(1) // use this to prevent child.Wait from returning immediately. ChildBuilt must be called
	c.WorkerStart()

	go func() {
		defer c.WorkerEnd()
		for {
			select {
			case err := <-child.Err:
				c.Err <- err
			case <-c.Quit:
				close(child.Quit)
				return
			case <-child.Done():
				return
			}
		}
	}()
	return child
}

// A DependencyGroup is used to define a wait group that will wait on
// all of the specified controllers to resolve.
//
// Calling Wait on the depenecy group will automatically denote the controller
// as built (if it is a child).
type DependencyGroup struct {
	ctrls []*Controller
}

// NewDependencyGroup builds a DependencyGroup with the specified controllers
func NewDependencyGroup(ctrls ...*Controller) *DependencyGroup {
	return &DependencyGroup{
		ctrls: ctrls,
	}
}

// SetCtrls sets the controllers for the DependencyGroup
func (d *DependencyGroup) SetCtrls(ctrls ...*Controller) {
	d.ctrls = ctrls
}

// Wait will automatically call child built on all specified controllers
// and return when all of the controllers have resolved
func (d *DependencyGroup) Wait() {
	if len(d.ctrls) == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(len(d.ctrls))
	for _, ctrl := range d.ctrls {
		go func(ctrl *Controller) {
			if ctrl.parent {
				ctrl.ChildBuilt()
			}
			ctrl.Wait()
			wg.Done()
		}(ctrl)
	}
	wg.Wait()
}
