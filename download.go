package ingest

import (
	"github.com/alexflint/go-cloudfile"
	"github.com/mcuadros/go-defaults"
	"io"
	"os"
	"path/filepath"
)

// DownloadCopyBlockBytes is how many bytes will be written between checking for aborts
var DownloadCopyBlockBytes int64 = 256000

// A Downloader will download the specified URLs
type Downloader struct {
	Opts     DownloadOpts
	Log      Logger
	URLs     []string
	URLCount int
}

// DownloadOpts are options used to configure a Downloader. They can be specified at contruction or via the Chainable API
type DownloadOpts struct {
	// The maximum number of concurrent downloads that will be allowed to run at the same time
	MaxParallelDownloads int `default:"1"`

	// DownloadTo is the path to the directory where downloads will be stored
	DownloadTo string `default:"tmp/"`

	// Progress is an optional channel that will receive DownloadProgress events
	Progress chan DownloadProgress
}

// DownloadProgress represents download progress
type DownloadProgress struct {
	File  string
	Bytes int
}

// NewDownloader builds a new Downloader
func NewDownloader() *Downloader {
	dl := &Downloader{
		Log: DefaultLogger.WithField("task", "download"),
	}
	defaults.SetDefaults(&dl.Opts)
	return dl
}

// Download creates a downloader which will download the specified urls
func Download(urls ...string) *Downloader {
	result := NewDownloader()
	result.URLs = urls
	result.URLCount = len(urls)
	return result
}

// Start starts running the Download task under the control of the passed in controller
func (d *Downloader) Start(ctrl *Controller) <-chan *os.File {
	result := make(chan *os.File)
	queue := d.downloadQueue()
	ctrl = ctrl.Child()
	defer ctrl.ChildBuilt()

	go func() {
		ctrl.Wait()
		close(result)
	}()

	for i := 0; i < d.Opts.MaxParallelDownloads; i++ {
		d.startDownloadWorker(ctrl, queue, result)
	}

	return result
}

func (d *Downloader) startDownloadWorker(ctrl *Controller, queue <-chan string, results chan *os.File) {
	d.Log.Debug("Starting download worker")
	ctrl.WorkerStart()
	go func() {
		defer ctrl.WorkerEnd()
		for {
			select {
			case <-ctrl.Quit:
				return
			case url, ok := <-queue:
				if !ok {
					return
				}
				res, err := d.DownloadURL(url, ctrl.Quit)
				if err != nil {
					ctrl.Err <- err
				} else {
					select {
					case <-ctrl.Quit:
						return
					case results <- res:
						continue
					}
				}
			}
		}
	}()
}

// DownloadURL will download the specified URL into the configured temp directory. If the URL
// is a file that exists on disk, the file will be read directly from the file system instead
func (d *Downloader) DownloadURL(url string, abort chan bool) (*os.File, error) {
	log := d.Log.WithField("file", url)
	log.Info("Opening...")

	reader, err := cloudfile.Open(url)
	if err != nil {
		log.WithError(err).Error("Error opening file")
		return nil, err
	}

	if asFile, isFile := reader.(*os.File); isFile {
		return asFile, nil
	}

	// If it is closable, close it after we are done copying the file to disk
	if asCloser, isCloser := reader.(io.Closer); isCloser {
		defer asCloser.Close()
	}

	_, outName := filepath.Split(url)
	destFile, err := os.Create(filepath.Join(d.Opts.DownloadTo, outName))
	if err != nil {
		log.WithError(err).Error("Error creating local file")
		return nil, err
	}

	for {
		select {
		case <-abort:
			return nil, ErrAborted
		default:
			if coppied, err := io.CopyN(destFile, reader, DownloadCopyBlockBytes); err != nil {
				d.reportProgress(outName, coppied)
				if err == io.EOF {
					return destFile, nil
				}
				log.WithError(err).Error("Error writing to local file")
				return nil, err
			}
		}
	}
}

func (d *Downloader) reportProgress(file string, bytes int64) {
	if d.Opts.Progress != nil {
		go func() {
			d.Opts.Progress <- DownloadProgress{File: file, Bytes: int(bytes)}
		}()
	}
}

func (d *Downloader) downloadQueue() <-chan string {
	queue := make(chan string, d.URLCount)
	for _, url := range d.URLs {
		queue <- url
	}
	close(queue)
	return queue
}
