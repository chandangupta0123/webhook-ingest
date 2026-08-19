# Solution

## 1. What was broken and why

The service had several correctness and reliability issues in the webhook
ingestion flow.

### 1.1 Duplicate webhook deliveries

The webhook provider can deliver the same event more than once, while the
`event_id` remains stable.

The original implementation used a check-then-insert flow:

1. Check whether `event_id` already exists.
2. If it does not exist, insert the event.
3. Update the call and account statistics.

This is vulnerable to a race condition. Two concurrent requests can both
observe that the event does not exist and both continue processing it.

This could result in duplicate event/call records and incorrect account
statistics.

### Fix

I added a PostgreSQL unique index on `events.event_id` and changed event
insertion to use:

    INSERT ... ON CONFLICT (event_id) DO NOTHING

This makes the database the final authority for event uniqueness and works
correctly even when multiple service instances receive the same webhook
concurrently.

---

## 2. Transactional consistency

The event, call record, and account statistics represent one logical
operation.

Previously, these operations could be performed independently. If one
operation failed after another succeeded, the database could be left in a
partially updated state.

### Fix

I wrapped the event insertion, call update, and account statistics update
inside a single PostgreSQL transaction.

The transaction is committed only when all operations succeed. If an
operation fails, the transaction is rolled back.

This keeps the durable state consistent.

---

## 3. Statistics cache concurrency

The in-memory statistics cache can be accessed by multiple HTTP requests
concurrently.

The cache read path was protected by a read lock, but the write/update path
was not consistently protected.

### Fix

I protected cache updates using the existing mutex.

This prevents concurrent goroutines from modifying the same map and
statistics structure unsafely.

---

## 4. Recording processing

Recording processing was performed asynchronously after receiving the
webhook.

The original background work could depend on the HTTP request context.
Once the request completed or was cancelled, the background operation could
also be cancelled.

Errors from recording processing were also not useful to operators because
they were not properly logged.

### Fix

Recording processing now uses its own timeout-based background context.

Recording failures are logged with useful identifiers such as event ID,
call ID, and account ID.

Background recording work is also tracked so graceful shutdown can wait for
in-flight work.

---

## 5. Deduplication strategy

I chose PostgreSQL as the source of truth for deduplication.

The service already stores events, calls, and account statistics in
PostgreSQL. Using a PostgreSQL uniqueness constraint means correctness
does not depend on in-memory state or a separate cache.

The unique constraint is:

    UNIQUE(event_id)

and insertion uses:

    ON CONFLICT (event_id) DO NOTHING

Redis can still be useful for caching or reducing database load, but I
would not make Redis the source of truth for correctness because PostgreSQL
already owns the durable event state.

---

## 6. Testing

I added regression tests for the failure scenarios, including duplicate
event processing and concurrent statistics cache access.

The tests are intended to demonstrate the problem before the fix and verify
that the corrected implementation remains correct under the same scenario.

I also used the Go race detector to identify unsafe concurrent access:

    go test -race ./...

---

## 7. What I would change at 10,000 webhooks/sec

At much higher traffic, I would separate webhook ingestion from
downstream processing.

The HTTP endpoint should acknowledge valid events quickly after durable
acceptance, while asynchronous processing should happen through a durable
queue or streaming system.

A possible architecture would be:

    Webhook
       |
       v
    HTTP API
       |
       v
    Durable Queue / Stream
       |
       +------> Workers
       |
       +------> Recording processing
       |
       v
    PostgreSQL

I would also consider:

- Horizontal scaling of webhook servers and workers.
- Partitioning or ordering by account/event key where required.
- Database connection-pool tuning.
- Batch processing where safe.
- Metrics for webhook throughput, duplicate rate, processing latency,
  queue depth, and failures.
- Distributed tracing for debugging production failures.
- Redis as a performance optimization rather than the correctness layer.

The main principle would remain the same: webhook processing must be
idempotent and durable even when requests are duplicated or multiple
instances process events concurrently.
