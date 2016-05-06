package write

import (
	"sync/atomic"
	"time"

	"github.com/mcuadros/go-defaults"
	"github.com/olivere/elastic"
	"github.com/urbint/ingest"
)

// ElasticWriterOpts are used to configure an ElasticWriter
type ElasticWriterOpts struct {
	NumWorkers        int           `default:"1"`
	MaxPendingActions int           `default:"3000"`
	FlushInterval     time.Duration `default:"3s"`
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
func ForElastic(in chan interface{}) chan ElasticWritable {
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

	return writer
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
			e.Log.WithFields(map[string]interface{}{
				"reason": esErr.Reason,
				"type":   esErr.Type,
			}).Error("Error in bulk insert")
		}
	}
	if err != nil {
		e.Log.WithError(err).Error("error writing to elasticsearch")
	}

	recsStored := atomic.LoadUint32(&e.pendingCount)
	atomic.StoreUint32(&e.pendingCount, 0)

	if e.Opts.Progress != nil {
		go func() {
			for i := uint32(0); i < recsStored; i++ {
				e.Opts.Progress <- struct{}{}
			}
		}()
	}

}
