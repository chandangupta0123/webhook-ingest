# Solution

## What was broken and why

The webhook ingestion flow had a race condition in event deduplication. It checked whether an `event_id` already existed and then inserted it separately. With concurrent or repeated deliveries, two requests could both observe the event as missing and process it, resulting in duplicate calls and incorrect account statistics.

The statistics cache also had unsafe concurrent updates. In addition, recording processing depended too much on the request lifecycle, so in-flight recording work could be cancelled during shutdown, and processing failures were not sufficiently visible in logs.

I added regression tests for the affected cases and fixed the underlying concurrency and lifecycle issues.

## Deduplication strategy

I chose PostgreSQL for deduplication because it is already the durable source of truth for events, calls, and account statistics. I added a unique constraint on `event_id` and use an atomic `INSERT ... ON CONFLICT DO NOTHING`.

I considered Redis, since it is already available, but using Redis as the correctness mechanism would add another state/consistency layer. PostgreSQL already guarantees uniqueness and works correctly across multiple application instances, so this approach is simpler and more reliable. Redis can still be useful later as a performance optimization.

## Scaling to 10,000 webhooks/second

At 10,000 webhooks/second, I would separate ingestion from downstream processing using a durable queue or streaming system. The HTTP layer would validate and durably accept events quickly, while horizontally scaled workers would process calls and recordings asynchronously.

I would also tune PostgreSQL connection pools, batch database operations where safe, partition work across workers, and add metrics for throughput, duplicate rate, latency, queue depth, and failures.
