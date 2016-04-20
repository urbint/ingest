package ingest

import (
	"sync"
)

// A Controller ...
type Controller struct {
	// Err is used by workers to report errors encountered while working
	Err chan error
	// Quit is closed by the controller when Abort is called
	Quit chan bool

	workerWg sync.WaitGroup
	parent   *Controller
}

// NewController builds a new Controller for use
func NewController() *Controller {
	ctrl := &Controller{
		Err:      make(chan error),
		Quit:     make(chan bool),
		workerWg: sync.WaitGroup{},
	}
	return ctrl
}

// ChildBuilt is callled on a child Controller to denote that it has been built and its wait group is valid
func (c *Controller) ChildBuilt() {
	if c.parent == nil {
		panic("ChildBuilt called on a non-child Controller")
	}
	c.workerWg.Done()
}

// WorkerStart is called by Worker to indicate to the controller
// that it is running and that the job is not complete until it exits
func (c *Controller) WorkerStart() *Controller {
	c.workerWg.Add(1)
	return c
}

// WorkerEnd is called by a Worker to signal that is has finished
// running
func (c *Controller) WorkerEnd() *Controller {
	c.workerWg.Done()
	return c
}

// Wait waits for all workers to have exited
func (c *Controller) Wait() {
	c.workerWg.Wait()
}

// Done returns a channel that will be closed when the worker controller has finished
func (c *Controller) Done() chan bool {
	done := make(chan bool)
	go func() {
		c.workerWg.Wait()
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

// Child creates a new child controller that will quit when the parent quits
// and report errors back to the parent.
//
// It will, however keep independent wait groups and all of its workers
// will only count as a single worker to its parent.
func (c *Controller) Child() *Controller {
	child := NewController()
	child.parent = c
	child.workerWg.Add(1) // use this to prevent child.Wait from returning immediately. ChildBuilt must be called
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
			}
		}
	}()
	return child
}
