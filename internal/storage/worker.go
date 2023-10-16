package storage

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const (
	Pending GroupState = iota + 1
	Committed
	Failed
)

var (
	ErrWorkerIsNotActive = errors.New("storage worker is not active")
)

type GroupState uint8

type Callback func(map[uint32]GroupState)

type Adapter[T any] interface {
	GetInsertQuery() string
	Values(T) []any
	GetCategoryID(T) uint32
}

type ClickhouseWorker[T any] struct {
	source           string
	conn             *sql.DB
	adapter          Adapter[T]
	maxBatchDuration time.Duration
	maxBatchSize     uint

	txLock    sync.RWMutex
	tx        *sql.Tx
	txStmt    *sql.Stmt
	commitsWG sync.WaitGroup

	ch       chan T
	chWg     sync.WaitGroup
	chActive bool
	chLock   sync.RWMutex

	stateLock  sync.RWMutex
	pending    map[uint32]uint
	executed   map[uint32]uint
	committing map[uint32]uint

	currentBatchSize uint

	callbacks []Callback
}

func NewClickhouseWorker[T any](source string, conn *sql.DB, adapter Adapter[T], maxBatchSize uint, maxBatchDuration time.Duration) *ClickhouseWorker[T] {
	return &ClickhouseWorker[T]{
		source:           source,
		conn:             conn,
		adapter:          adapter,
		maxBatchSize:     maxBatchSize,
		maxBatchDuration: maxBatchDuration,

		pending:    make(map[uint32]uint, maxBatchSize+maxBatchSize),
		executed:   make(map[uint32]uint, maxBatchSize),
		committing: make(map[uint32]uint, maxBatchSize),
	}
}

func (w *ClickhouseWorker[T]) Start(ctx context.Context) error {
	ticker := time.NewTicker(w.maxBatchDuration)

	w.chLock.Lock()
	w.chActive = true
	w.ch = make(chan T, w.maxBatchSize+w.maxBatchSize)
	w.chLock.Unlock()

	w.createNewTxUnsafe()

	// Run goroutine for reading items from the channel
	go w.processItems()

	for {
		select {
		case <-ctx.Done():
			w.chLock.Lock()
			w.chActive = false
			close(w.ch)
			w.chLock.Unlock()

			// waiting for reading all events from the channel
			w.chWg.Wait()

			w.commit()

			w.commitsWG.Wait()

			err := ctx.Err()
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return err
		case <-ticker.C:
			w.commitAndCreateNewTx()
		}
	}
}

func (w *ClickhouseWorker[T]) RegisterCallback(cb Callback) {
	w.callbacks = append(w.callbacks, cb)
}

func (w *ClickhouseWorker[T]) commit() {
	w.txLock.Lock()
	defer w.txLock.Unlock()

	w.commitUnsafe()
}

func (w *ClickhouseWorker[T]) commitAndCreateNewTx() {
	w.txLock.Lock()
	defer w.txLock.Unlock()

	w.commitUnsafe()
	w.createNewTxUnsafe()
}

func (w *ClickhouseWorker[T]) commitUnsafe() {
	w.commitsWG.Add(1)
	go func(tx *sql.Tx, count uint) {
		defer observe(histCommitDuration, w.source, time.Now())

		w.stateLock.Lock()
		groups := make(map[uint32]GroupState, len(w.executed))
		for group := range w.executed {
			status := Committed
			if w.pending[group] > 0 {
				status = Pending
			}

			groups[group] = status
		}
		w.executed = make(map[uint32]uint, w.maxBatchSize)
		w.stateLock.Unlock()

		log.Info().
			Str("source", w.source).
			Uint("count_records", count).
			Msg("commit batch to the clickhouse")

		if err := tx.Commit(); err != nil {
			log.Error().
				Err(err).
				Str("source", w.source).
				Uint("count_records", count).
				Msg("unable to commit transaction to the clickhouse")

			failedGroups := make(map[uint32]GroupState, len(groups))
			for blockNumKey := range groups {
				failedGroups[blockNumKey] = Failed
			}
			groups = failedGroups
		}
		w.commitsWG.Done()

		if len(groups) == 0 {
			return
		}

		for _, cb := range w.callbacks {
			cb(groups)
		}
	}(w.tx, w.currentBatchSize)

	w.currentBatchSize = 0
}

func (w *ClickhouseWorker[T]) createNewTxUnsafe() {
	defer observe(histNewTxDuration, w.source, time.Now())

	tx, err := w.conn.Begin()
	if err != nil {
		log.Error().
			Err(err).
			Msg("unable to start new transaction in the clickhouse")
		panic(err)
	}

	stmt, err := tx.Prepare(w.adapter.GetInsertQuery())
	if err != nil {
		log.Error().
			Err(err).
			Msg("unable to start new transaction in the clickhouse")
		panic(err)
	}

	w.tx = tx
	w.txStmt = stmt
}

func (w *ClickhouseWorker[T]) processItems() {
	for item := range w.ch {
		w.txLock.RLock()

		txesCounter.WithLabelValues(w.source).Inc()

		w.stateLock.Lock()
		categoryID := w.adapter.GetCategoryID(item)
		w.executed[categoryID]++
		w.pending[categoryID]--
		if w.pending[categoryID] <= 0 {
			delete(w.pending, categoryID)
		}
		w.stateLock.Unlock()

		_, err := w.txStmt.Exec(w.adapter.Values(item)...)
		if err != nil {
			log.Error().
				Err(err).
				Msg("unable to execute prepared statement")
		}
		w.currentBatchSize++
		w.txLock.RUnlock()
		w.chWg.Done()

		if w.currentBatchSize >= w.maxBatchSize {
			w.commitAndCreateNewTx()
		}
	}
}

func (w *ClickhouseWorker[T]) Store(group uint32, items ...T) error {
	w.chLock.RLock()
	defer w.chLock.RUnlock()

	if !w.chActive {
		return ErrWorkerIsNotActive
	}

	if len(items) == 0 {
		for _, cb := range w.callbacks {
			cb(map[uint32]GroupState{
				group: Committed,
			})
		}

		return nil
	}

	w.stateLock.Lock()
	w.pending[group] += uint(len(items))
	w.stateLock.Unlock()

	w.chWg.Add(len(items))
	for _, item := range items {
		w.ch <- item
	}

	return nil
}

func observe(hist *prometheus.HistogramVec, source string, startTime time.Time) {
	hist.WithLabelValues(source).Observe(float64(time.Since(startTime) / time.Millisecond))
}
