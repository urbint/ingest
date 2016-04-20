package ingest

import "errors"

// ErrAborted is returned when an abortable task is aborted
var ErrAborted = errors.New("Task was aborted")
