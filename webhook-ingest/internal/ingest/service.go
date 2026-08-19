// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

const recordingTimeout = 30 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	workers sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}
 

// Ingest stores a delivery and kicks off recording processing. Durable event,
// call, and account-stat changes are committed together. Duplicate event_ids
// are a successful no-op, which makes provider retries safe.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.ProcessEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	// The database transaction is committed before the cache is changed, so a
	// failed database transaction can never make the in-memory aggregate drift.
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recording work must not depend on the lifetime of the HTTP request. A
	// deployment using graceful shutdown waits for these workers to finish.
	if rec.RecordingURL != "" {
		s.workers.Add(1)
		go s.recordRecording(rec)
	}

	return nil
}

func (s *Service) recordRecording(rec store.Event) {
	defer s.workers.Done()

	ctx, cancel := context.WithTimeout(context.Background(), recordingTimeout)
	defer cancel()

	if err := s.processRecording(ctx, rec); err != nil {
		s.log.Error("recording processing failed",
			"event_id", rec.EventID,
			"call_id", rec.CallID,
			"account_id", rec.AccountID,
			"err", err,
		)
		return
	}

	s.log.Info("recording processed",
		"event_id", rec.EventID,
		"call_id", rec.CallID,
	)
}

// Shutdown waits for background recording work to finish. The caller should
// stop accepting new HTTP requests before calling this method.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	timer := time.NewTimer(recordingWork)
	defer timer.Stop()

	select {
	case <-timer.C:
		return s.store.MarkRecordingProcessed(ctx, rec.CallID)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
