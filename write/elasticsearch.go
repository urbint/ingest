package write

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/mcuadros/go-defaults"
	"github.com/olivere/elastic"
	"github.com/urbint/ingest"
	"github.com/urbint/ingest/utils"
)

// ElasticWriterOpts are used to configure an ElasticWriter
type ElasticWriterOpts struct {
	NumWorkers        int           // defaults to runtime.NumCPU()
	MaxPendingActions int           `default:"-1"`
	FlushInterval     time.Duration `default:"5m"`
	FlushSize         int           `default:"15000000"`
	AbortOnError      bool          `default:"false"`
	Progress          chan struct{}
}

// An ElasticWriter writes records to Elasticsearch
type ElasticWriter struct {
	Opts ElasticWriterOpts

	In  <-chan ElasticWritable
	Log ingest.Logger

	errs chan error
	es   *elastic.Client

	processor    *elastic.BulkProcessor
	pendingCount uint32
}

// ElasticWritable is an interface that a record must implement to be stored in Elasticsearch
type ElasticWritable interface {

	// ForElastic returns a record for elasticsearch, specifying the index, type, id and data.
	//
	// The data will be handed to json.Marshal. If data is nil the record will not be inserted
	ForElastic() (index string, elasticType string, id string, data interface{})
}

// ForElastic converts a chan of interface to a chan of ElasticWritable
func ForElastic(in <-chan interface{}) chan ElasticWritable {
	result := make(chan ElasticWritable)
	go func() {
		for rec := range in {
			result <- rec.(ElasticWritable)
		}
		close(result)
	}()
	return result
}

// Elasticsearch returns a new Writer which will store records into elasticsearch
func Elasticsearch(client *elastic.Client, input <-chan ElasticWritable) *ElasticWriter {
	writer := &ElasticWriter{
		In:   input,
		Log:  ingest.DefaultLogger.WithField("task", "write-elasticsearch"),
		es:   client,
		errs: make(chan error),
	}

	defaults.SetDefaults(&writer.Opts)

	writer.Opts.NumWorkers = runtime.NumCPU()

	return writer
}

// ReportProgressTo is a chainable configuration method that sets where
// progress will be reported to
func (e *ElasticWriter) ReportProgressTo(dest chan struct{}) *ElasticWriter {
	e.Opts.Progress = dest
	return e
}

// NumWorkers is a chainable configuration method that sets how
// many workers are used to write to elasticsearch
func (e *ElasticWriter) NumWorkers(count int) *ElasticWriter {
	e.Opts.NumWorkers = count
	return e
}

// MaxPending is a chainable configuration method that sets how
// many queued operations the bulk-processor will allow before sending a request
//
// Setting it to -1 will disable flushing based off max pending ops
func (e *ElasticWriter) MaxPending(count int) *ElasticWriter {
	e.Opts.MaxPendingActions = count
	return e
}

// FlushEvery is a chainable configuration method that sets the interval which
// will force a Flush of the queued bulk-processor operations
//
// Setting it to -1 will disable flushing based off an interval
func (e *ElasticWriter) FlushEvery(interval time.Duration) *ElasticWriter {
	e.Opts.FlushInterval = interval
	return e
}

// FlushSize is a chainable configuration method that sets the max
// size of queued record operations that will trigger a flush of
// the bulk-processor
//
// Setting it to -1 will disable flushing based off max record size
func (e *ElasticWriter) FlushSize(numBytes int) *ElasticWriter {
	e.Opts.FlushSize = numBytes
	return e
}

// ApplySettings reads the configured SettingsPath and applies the settings file to
// elasticsearch cluster.
func (e *ElasticWriter) ApplySettings(indexName string, settingsPath string) error {
	settings, err := utils.ReadConfig(settingsPath)
	if err != nil {
		return err
	}

	_, err = e.es.IndexPutSettings(indexName).BodyJson(settings).Do()

	return err
}

// CreateIndexWithSettings creates an index with the specified name and settings. If the index already
// exists and recreate is true, it will delete the index before creating it
func (e *ElasticWriter) CreateIndexWithSettings(indexName string, settingsPath string, recreate bool) error {
	exists, err := e.es.IndexExists(indexName).Do()
	if !exists && err != nil {
		return err
	}

	settings, err := utils.ReadConfig(settingsPath)
	if err != nil {
		return err
	}

	if exists && !recreate {
		return nil
	}

	if recreate {
		if _, err = e.es.DeleteIndex(indexName).Do(); err != nil {
			return err
		}
	}

	_, err = e.es.CreateIndex(indexName).BodyJson(settings).Do()

	return err
}

// Start starts the ElasticWriter under the control of the *ingest.Controller
func (e *ElasticWriter) Start(ctrl *ingest.Controller) {
	if err := e.startBulkProcessor(); err != nil {
		ctrl.Err <- err
		return
	}
	defer e.stopBulkProcessor()

	for {
		select {
		case <-ctrl.Quit:
			return
		case err := <-e.errs:
			ctrl.Err <- err
			if e.Opts.AbortOnError {
				return
			}
		case rec, ok := <-e.In:
			if !ok {
				return
			}
			e.storeRec(rec)
		}
	}
}

// startBulkProcessor starts the elastic.BulkProcessor
func (e *ElasticWriter) startBulkProcessor() error {
	e.Log.Debug("Starting BulkInserter")
	proc, err := e.es.BulkProcessor().
		Workers(e.Opts.NumWorkers).
		BulkActions(e.Opts.MaxPendingActions).
		BulkSize(e.Opts.FlushSize).
		FlushInterval(e.Opts.FlushInterval).
		After(e.afterFlush).
		Do()

	if err != nil {
		return err
	}

	e.processor = proc
	return nil
}

// stopBulkProcessor flushes any pending records and returns
func (e *ElasticWriter) stopBulkProcessor() error {
	if err := e.processor.Flush(); err != nil {
		return err
	}
	return e.processor.Stop()
}

func (e *ElasticWriter) storeRec(rec ElasticWritable) {
	index, elasticType, id, data := rec.ForElastic()
	if data == nil {
		return
	}

	e.processor.Add(elastic.NewBulkIndexRequest().Index(index).Type(elasticType).Id(id).Doc(data))
	atomic.AddUint32(&e.pendingCount, 1)
}

func (e *ElasticWriter) afterFlush(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if response.Errors {
		for _, item := range response.Failed() {
			esErr := item.Error
			e.Log.WithField("reason", esErr.Reason).WithField("type", esErr.Type).Error("Error in bulk insert")
		}
	}
	if err != nil {
		e.Log.WithError(err).Error("error writing to elasticsearch")
	}

	recsStored := atomic.LoadUint32(&e.pendingCount)
	e.Log.WithField("recsStored", recsStored).Debug("ElasticSearch flushed")
	atomic.StoreUint32(&e.pendingCount, 0)

	if e.Opts.Progress != nil {
		go func() {
			for i := uint32(0); i < recsStored; i++ {
				e.Opts.Progress <- struct{}{}
			}
		}()
	}

}
