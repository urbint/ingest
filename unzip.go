package ingest

import (
	"archive/zip"
	"github.com/mcuadros/go-defaults"
	"io"
	"os"
	"path/filepath"
)

// An Unzipper will download and extract the specified URLs
type Unzipper struct {
	URLs     []string
	URLCount int
	Log      Logger
	Opts     UnzipperOpts

	depGroup *DependencyGroup
	ctrl     *Controller
}

// UnzipperOpts are options used to configure an Unzipper. They can be specified
// at contruction or via the Chainable API
type UnzipperOpts struct {
	DownloadOpts
	Progress          chan UnzipProgress
	MaxParallelUnzips int `default:"1"`
	Filter            string
}

// UnzipProgress represents Progress encountered while unzipping.
type UnzipProgress struct {
	// FileName is the name of the source zip file
	FileName string

	// ContentsCount is the number of files emitted from extracting the Zip.
	// If a filter is set, the count reflects the number after the filter is applied.
	ContentsCount int
}

// NewUnzipper builds a new Unzipper. Generally you will want to use the shortcut method `Unzip`
func NewUnzipper() *Unzipper {
	unzipper := &Unzipper{
		Log:      DefaultLogger.WithField("task", "unzip"),
		depGroup: NewDependencyGroup(),
	}

	defaults.SetDefaults(&unzipper.Opts)
	return unzipper
}

// Unzip creates an Unzipper which will unzip the specified URLs
func Unzip(urls ...string) *Unzipper {
	result := NewUnzipper()
	result.URLs = urls
	result.URLCount = len(urls)
	return result
}

// Start starts running the Unzip task under the control of the specified controller
func (u *Unzipper) Start(ctrl *Controller) <-chan io.ReadCloser {
	ctrl = ctrl.Child()
	defer ctrl.ChildBuilt()

	u.depGroup.Wait()

	unzipped := make(chan io.ReadCloser)
	go func() {
		ctrl.Wait()
		close(unzipped)
	}()

	files := Download(u.URLs...).WithOpts(u.Opts.DownloadOpts).Start(ctrl)

	for i := 0; i < u.Opts.MaxParallelUnzips; i++ {
		u.startUnzipWorker(ctrl, files, unzipped)
	}

	return unzipped
}

// Filter sets a filepath.Match pattern that will be used to filter the results
// from the unzipper.
//
// It returns the unzipper for a chainable API
func (u *Unzipper) Filter(pattern string) *Unzipper {
	u.Opts.Filter = pattern
	return u
}

// Cleanup is a chainable configuration method to set whether the directory referred to
// by DownloadOpts.DownloadTo will be removed when the invoking controller finishes
func (u *Unzipper) Cleanup(cleanup bool) *Unzipper {
	u.Opts.Cleanup = cleanup
	return u
}

// ReportDownloadProgressTo is a chainable configuration method to set where download
// progress is reported to.
func (u *Unzipper) ReportDownloadProgressTo(progress chan DownloadProgress) *Unzipper {
	u.Opts.DownloadOpts.Progress = progress
	return u
}

// ReportProgressTo is a chainable configuration method to set where unzip
// progress is reported to
func (u *Unzipper) ReportProgressTo(progress chan UnzipProgress) *Unzipper {
	u.Opts.Progress = progress
	return u
}

// DependOn is a chainable configuration method that will not proceed until all
// specified controllers have resolved
func (u *Unzipper) DependOn(ctrls ...*Controller) *Unzipper {
	u.depGroup.SetCtrls(ctrls...)
	return u
}

func (u *Unzipper) startUnzipWorker(ctrl *Controller, input <-chan *os.File, output chan<- io.ReadCloser) {
	ctrl.WorkerStart()
	u.Log.Debug("Starting worker")
	go func() {
		defer ctrl.WorkerEnd()
		defer u.Log.Debug("Exiting worker")
		for {
			select {
			case <-ctrl.Quit:
				return
			case file, ok := <-input:
				if !ok {
					return
				}
				results, err := u.UnzipFile(file)
				if err != nil {
					ctrl.Err <- err
				} else {
					for _, result := range results {
						select {
						case <-ctrl.Quit:
							return
						case output <- result:
							continue
						}
					}
				}
			}
		}
	}()
}

// UnzipFile will unzip the specified os.File and return an array of ReadClosers
//
// The file will be closed as a result of being passed to Unzip
func (u *Unzipper) UnzipFile(file *os.File) ([]io.ReadCloser, error) {
	result := []io.ReadCloser{}

	file.Close()
	archive, err := zip.OpenReader(file.Name())
	if err != nil {
		return nil, err
	}

	for _, inside := range archive.File {
		name := inside.FileHeader.Name
		fileLog := u.Log.WithField("file", name)
		if u.filterMatch(name) {
			fileLog.Debug("Found file")
			opened, err := inside.Open()
			if err != nil {
				// We errored, close all of the open files before returning the error
				for _, file := range result {
					file.Close()
				}
				return nil, err
			}
			result = append(result, opened)
		} else {
			fileLog.Debug("Skipping file")
		}
	}

	u.reportProgress(file.Name(), len(result))

	return result, nil
}

// filterMatch will return whether the specified file name matches the configured filter
func (u *Unzipper) filterMatch(fileName string) bool {
	if u.Opts.Filter == "" {
		return true
	}
	res, err := filepath.Match(u.Opts.Filter, fileName)
	if err != nil {
		u.Log.WithField("pattern", u.Opts.Filter).WithError(err).Warn("Invalid file pattern")
		return false
	}
	return res
}

// reportProgress will report unzip progress
func (u *Unzipper) reportProgress(file string, contentCount int) {
	if u.Opts.Progress != nil {
		go func() {
			u.Opts.Progress <- UnzipProgress{file, contentCount}
		}()
	}
}
