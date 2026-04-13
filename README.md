# PgQue -- PgQ Universal Edition

**Zero-bloat PostgreSQL queue. No extensions. No daemons. One SQL file.**

[![CI](https://github.com/NikolayS/pgque/actions/workflows/ci.yml/badge.svg)](https://github.com/NikolayS/pgque/actions/workflows/ci.yml)
[![PostgreSQL 14-18](https://img.shields.io/badge/PostgreSQL-14--18-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![pg_cron](https://img.shields.io/badge/pg__cron-%E2%89%A51.5-336791)](https://github.com/citusdata/pg_cron)

PgQue is a repackaging of [PgQ](https://github.com/pgq/pgq) -- the
battle-tested queue system that ran at Skype/Microsoft scale for 15+ years --
into a modern, extension-free system that works on any managed PostgreSQL
provider.

## Why PgQue

Every other PostgreSQL queue uses `SKIP LOCKED` + `DELETE`, which creates dead
tuples. Under sustained load, VACUUM can't keep up, indexes bloat, and
throughput collapses. PgQue is **structurally immune** -- it uses TRUNCATE-based
table rotation instead of per-row deletion. Zero dead tuples, ever.

| Feature | PgQue | PGMQ | River | graphile-worker | pg-boss | Oban |
|---|---|---|---|---|---|---|
| Claim mechanism | Snapshot isolation (lockless) | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED |
| Table bloat under load | **None** (TRUNCATE rotation) | Yes (DELETE) | Yes (UPDATE/DELETE) | Yes (UPDATE/DELETE) | Mitigated (partitioned) | Yes (UPDATE/DELETE) |
| Requires C extension | No | Yes (Rust) | No (Go binary) | No | No | No |
| Language-agnostic | **Yes** (SQL API) | Yes (SQL API) | Go only | Node.js only | Node.js only | Elixir only |
| Managed PG compatible | **Yes** | Depends (ext) | Yes | Yes | Yes | Yes |
| Multiple consumers | **Built-in** | Manual | No | No | No | Via queues |
| DLQ | **Built-in** | Via archival | No | No | Built-in | Via plugin |
| Battle-tested | **15+ years** (Skype/MS) | ~2 years | ~2 years | ~5 years | ~5 years | ~5 years |

## Installation

**Requirements:** PostgreSQL 14+ and optionally pg_cron >= 1.5 (available on
all major managed providers).

```sql
-- One-file install -- no make, no CREATE EXTENSION, no server restart
\i pgque-install.sql

-- Start the ticker and maintenance jobs (requires pg_cron)
select pgque.start();
```

That's it. Works on RDS, Aurora, AlloyDB, Cloud SQL, Supabase, Neon, and
Crunchy Bridge -- no DBA needed.

The install is **idempotent** -- safe to run multiple times. To uninstall:

```sql
select pgque.uninstall();  -- stops pg_cron jobs, drops schema
```

<details>
<summary>Without pg_cron</summary>

If pg_cron is unavailable (e.g., serverless scale-to-zero), call the ticker
and maintenance functions from any external scheduler:

```bash
# cron, systemd timer, or application loop
psql -c "select pgque.ticker()"   # every 1-2 seconds
psql -c "select pgque.maint()"    # every 30 seconds
```
</details>

## Quick Start

```sql
-- 1. Create a queue
select pgque.create_queue('orders');

-- 2. Register a consumer
select pgque.subscribe('orders', 'processor');

-- 3. Send a message
select pgque.send('orders', '{"order_id": 42, "total": 99.95}'::jsonb);

-- Wait for the next tick (1-2 seconds by default)

-- 4. Receive and process messages
select * from pgque.receive('orders', 'processor', 100);
--  msg_id | batch_id | type | payload                              | retry_count | created_at          | ...
-- --------+----------+------+--------------------------------------+-------------+---------------------+----
--       1 |        1 |      | {"order_id": 42, "total": 99.95}     |             | 2026-04-13 10:00:01 |

-- 5. Acknowledge the batch
select pgque.ack(1);  -- batch_id from step 4
```

## Usage Examples

### Send with event type

Route different event kinds through a single queue using `ev_type`:

```sql
select pgque.send('orders', 'order.created',
  '{"order_id": 42}'::jsonb);

select pgque.send('orders', 'order.shipped',
  '{"order_id": 42, "tracking": "1Z999AA10123456784"}'::jsonb);
```

### Batch send

Insert many messages atomically in a single transaction:

```sql
select pgque.send_batch('orders', 'order.created', array[
  '{"order_id": 1}'::jsonb,
  '{"order_id": 2}'::jsonb,
  '{"order_id": 3}'::jsonb
]);
```

### Delayed delivery

Schedule a message for future delivery:

```sql
select pgque.send_at('reminders', 'reminder.send',
  '{"user_id": 7}'::jsonb,
  now() + interval '24 hours');
```

### Retry and dead letter queue

When processing fails, `nack()` schedules a retry. After exceeding
`max_retries`, the message is automatically moved to the dead letter queue:

```sql
-- Processing failed -- retry in 60 seconds
select pgque.nack(batch_id, msg, '60 seconds'::interval, 'timeout from upstream');

-- Inspect dead-lettered messages
select * from pgque.dlq_inspect('orders');

-- Replay a single message back to the queue
select pgque.dlq_replay(dead_letter_id);

-- Replay all dead-lettered messages
select pgque.dlq_replay_all('orders');

-- Purge old DLQ entries
select pgque.dlq_purge('orders', '30 days'::interval);
```

### Fan-out (multiple consumers)

Each consumer tracks its own position independently:

```sql
select pgque.subscribe('orders', 'audit_logger');
select pgque.subscribe('orders', 'notification_sender');
select pgque.subscribe('orders', 'analytics_pipeline');

-- Each consumer receives the same events independently
select * from pgque.receive('orders', 'audit_logger', 100);
select * from pgque.receive('orders', 'notification_sender', 100);
```

### Transactional exactly-once processing

Wrap receive, application writes, and ack in a single transaction:

```sql
begin;
  select * from pgque.receive('orders', 'processor', 100) into temp msgs;

  insert into processed_orders (order_id, status)
  select (payload::jsonb->>'order_id')::int, 'done'
  from msgs;

  select pgque.ack((select distinct batch_id from msgs limit 1));
commit;
-- If anything fails, the entire TX rolls back --
-- application writes AND ack are both undone.
```

### Queue configuration

Tune queue parameters at creation time:

```sql
select pgque.create_queue('high_volume', '{
  "rotation_period": "4 hours",
  "ticker_max_count": 1000,
  "ticker_max_lag": "5 seconds",
  "max_retries": 10
}'::jsonb);
```

### Observability

```sql
-- Real-time queue health
select * from pgque.queue_stats();
--  queue_name | depth | oldest_msg_age | consumers | events_per_sec | dlq_count | ...

-- Operational diagnostics
select * from pgque.queue_health();
--  queue_name | check_name      | status   | detail
-- ------------+-----------------+----------+-----------------------------
--  orders     | ticker_running  | ok       | Last tick 1.2s ago
--  orders     | consumer_lag    | warning  | processor: lag 45 min

-- Per-consumer metrics
select * from pgque.consumer_stats();

-- Stuck consumer detection
select * from pgque.stuck_consumers('1 hour'::interval);

-- OpenTelemetry-compatible metrics (scrape via sql_exporter or OTel collector)
select * from pgque.otel_metrics();
--  metric_name                              | metric_type | value | labels
-- ------------------------------------------+-------------+-------+----------------------------
--  pgque.queue.depth                        | gauge       |  1204 | {"queue": "orders"}
--  pgque.consumer.lag_seconds               | gauge       |  3.12 | {"queue": "orders", ...}
```

### Recurring jobs with pg_cron

```sql
select cron.schedule('daily_report',
  '0 9 * * *',
  $$select pgque.send('jobs', 'report.generate',
      '{"type": "daily"}'::jsonb)$$);
```

## Client Libraries

### Python (pgque-py)

Built on psycopg 3. Decorator-based consumer with LISTEN/NOTIFY wakeup:

```python
from pgque import PgqueClient, Consumer

# Producer
client = PgqueClient(conn)
msg_id = client.send("orders", {"order_id": 42})
msg_ids = client.send_batch("orders", "order.created", [
    {"order_id": 1},
    {"order_id": 2},
])

# Consumer
consumer = Consumer(dsn, queue="orders", name="processor", poll_interval=30)

@consumer.on("order.created")
def handle_order(msg):
    process_order(msg.payload)

@consumer.on("order.created", transactional=True)
def handle_order_exactly_once(msg, conn):
    conn.execute(
        "insert into processed_orders (order_id) values (%s)",
        [msg.payload["order_id"]],
    )
    # auto-committed with ack if no exception

consumer.start()  # blocks until SIGTERM/SIGINT
```

### Go (pgque-go)

Built on pgx/v5. Context-aware with structured errors:

```go
client, _ := pgque.Connect(ctx, "postgresql://localhost/mydb")

// Producer
eid, _ := client.Send(ctx, "orders", pgque.Event{
    Type:    "order.created",
    Payload: Order{ID: 42},
})

// Consumer
consumer := client.NewConsumer("orders", "processor",
    pgque.WithPollInterval(30*time.Second))

consumer.Handle("order.created", func(ctx context.Context, msg pgque.Message) error {
    return processOrder(msg)
})

consumer.Start(ctx)  // blocks until context cancelled
```

### Any language (SQL API)

PgQue's API is pure SQL -- any language with a PostgreSQL driver works:

```sql
select pgque.send('orders', '{"order_id": 42}'::jsonb);
select * from pgque.receive('orders', 'processor', 100);
select pgque.ack(batch_id);
```

## Function Reference

### Publishing

| Function | Returns | Description |
|---|---|---|
| `pgque.send(queue, payload)` | `bigint` | Send a message with default type |
| `pgque.send(queue, type, payload)` | `bigint` | Send with explicit event type |
| `pgque.send_batch(queue, type, payloads[])` | `bigint[]` | Batch send (single transaction) |
| `pgque.send_at(queue, type, payload, deliver_at)` | `bigint` | Delayed/scheduled delivery |

### Consuming

| Function | Returns | Description |
|---|---|---|
| `pgque.receive(queue, consumer, max_return)` | `setof pgque.message` | Receive up to N messages from next batch |
| `pgque.ack(batch_id)` | `integer` | Finish batch, advance consumer position |
| `pgque.nack(batch_id, msg, retry_after, reason)` | `integer` | Retry or route to DLQ if max retries exceeded |
| `pgque.subscribe(queue, consumer)` | `integer` | Register a consumer |
| `pgque.unsubscribe(queue, consumer)` | `integer` | Unregister a consumer |

### Dead Letter Queue

| Function | Returns | Description |
|---|---|---|
| `pgque.dlq_inspect(queue, limit)` | `setof pgque.dead_letter` | Inspect DLQ entries |
| `pgque.dlq_replay(dead_letter_id)` | `bigint` | Replay single event back to queue |
| `pgque.dlq_replay_all(queue)` | `integer` | Replay all DLQ events for a queue |
| `pgque.dlq_purge(queue, older_than)` | `integer` | Purge old DLQ entries |

### Queue Management

| Function | Returns | Description |
|---|---|---|
| `pgque.create_queue(queue)` | `integer` | Create a new queue |
| `pgque.create_queue(queue, options)` | `integer` | Create queue with JSONB options |
| `pgque.drop_queue(queue)` | `integer` | Drop a queue |
| `pgque.pause_queue(queue)` | `void` | Pause ticker for a queue |
| `pgque.resume_queue(queue)` | `void` | Resume ticker for a queue |

### Lifecycle

| Function | Returns | Description |
|---|---|---|
| `pgque.start()` | -- | Create pg_cron ticker + maintenance jobs |
| `pgque.stop()` | -- | Remove pg_cron jobs |
| `pgque.status()` | `table` | Diagnostic dashboard (ticker, pg version, etc.) |
| `pgque.uninstall()` | -- | Stop pg_cron jobs and `DROP SCHEMA pgque CASCADE` |

### Observability

| Function | Returns | Description |
|---|---|---|
| `pgque.queue_stats()` | `table` | Depth, consumer count, events/sec, DLQ count per queue |
| `pgque.consumer_stats()` | `table` | Lag, pending events, batch status per consumer |
| `pgque.queue_health()` | `table` | Operational diagnostics (ok / warning / critical) |
| `pgque.otel_metrics()` | `table` | OTel-compatible gauges (depth, lag, DLQ, throughput) for sql_exporter / OTel collector |
| `pgque.throughput(queue, period, bucket)` | `table` | Events per second over time |
| `pgque.latency_percentiles(queue, consumer, period)` | `table` | p50, p95, p99 latency |
| `pgque.error_rate(queue, period, bucket)` | `table` | Retries and dead letters over time |
| `pgque.in_flight(queue)` | `table` | Currently-processing batches |
| `pgque.stuck_consumers(threshold)` | `table` | Consumers that haven't processed recently |

### PgQ-Native API

The full PgQ primitive API remains available for advanced use:

| Function | Description |
|---|---|
| `pgque.insert_event(queue, type, data)` | Low-level event insert with `ev_extra1..4` support |
| `pgque.next_batch(queue, consumer)` | Get next batch ID |
| `pgque.get_batch_events(batch_id)` | Get all events in a batch |
| `pgque.finish_batch(batch_id)` | Mark batch complete |
| `pgque.event_retry(batch_id, event_id, seconds)` | Schedule event retry |

## Benchmarks

Preliminary results on a laptop (Apple Silicon, 10 cores, 24 GiB RAM,
PostgreSQL 18.3, `synchronous_commit=off` per-session). Full methodology:
[NikolayS/pgq#1](https://github.com/NikolayS/pgq/issues/1).

| Scenario | Throughput | Per core |
|---|---|---|
| PL/pgSQL single insert/TX, ~100 B, 16 clients | **85,836 ev/s** | ~8.6k ev/s |
| PL/pgSQL batched 100k/TX, ~100 B | 80,515 ev/s | ~8.1k ev/s |
| PL/pgSQL batched 100k/TX, ~2 KiB | 48,899 ev/s (91.5 MiB/s) | ~4.9k ev/s |
| C mode batched 1000/TX, ~100 B | 417,414 ev/s | ~41.7k ev/s |
| Consumer read rate, 100k batch, ~100 B | ~2.4M ev/s | ~240k ev/s |
| Consumer read rate, 100k batch, ~2 KiB | ~305k ev/s (568 MiB/s) | ~30.5k ev/s |

The PL/pgSQL rows reflect pgque's pure-SQL mode. Key takeaways:

- **Zero bloat under sustained load** -- 30-minute sustained test (70
  checkpoints) showed zero dead tuple growth in event tables
- **Tuning > language** -- PL/pgSQL tuned (86k ev/s) beats C untuned (52k ev/s)
- **Batching matters most** -- 1000 inserts/TX reaches 417k ev/s (3.6x over
  single-insert)
- **Consumer is never the bottleneck** -- read rate is 3-6x faster than write
  rate
- **~1/3 of RedPanda per-core throughput** (~30 MiB/s vs ~100 MiB/s), but with
  full ACID transactions and zero bloat

> **Note:** `synchronous_commit=off` is settable per-session or per-transaction
> -- safe for queue workloads even when the global setting is `on`, since at
> worst the last few ms of committed events are lost on crash.

## Architecture

PgQue uses PgQ's proven architecture:

- **Snapshot-based batch isolation** -- each batch contains exactly the events
  committed between two ticks. No gaps, no duplicates.
- **3-table TRUNCATE rotation** -- event tables rotate via TRUNCATE (DDL, not
  DML). Zero dead tuples, zero VACUUM pressure.
- **Multiple independent consumers** -- each consumer tracks its own position.
  One queue, many readers.
- **pg_cron ticker** -- replaces PgQ's external `pgqd` daemon. Runs as
  scheduled SQL inside the database (every 2 seconds).

See [SPECx.md](blueprints/SPECx.md) for the full specification and
[SPEC.md](blueprints/SPEC.md) for PgQ internals (rotation mechanics, snapshot
isolation, batch algorithm).

## When NOT to Use PgQue

Be honest about the trade-offs:

- **Sub-10ms latency** -- pgque's tick-based architecture means typical latency
  is 1-2 seconds (~100ms with LISTEN/NOTIFY). For sub-10ms, use
  graphile-worker or direct LISTEN/NOTIFY.
- **100k+ ev/s sustained** -- if you need sustained throughput beyond ~86k ev/s,
  consider a dedicated broker (Kafka, RedPanda).
- **Complex multi-step workflows** -- if your workload is branching stateful
  processes, use a workflow engine (Temporal, Restate, Absurd).
- **Single-ecosystem teams** -- if you're pure Go, River has better DX. Pure
  Elixir, Oban is standard. Rails 8, solid_queue is built in. PgQue shines in
  polyglot stacks and where zero-bloat matters.

## Roadmap

| Milestone | Focus | Status |
|---|---|---|
| Sprint 1 | Repackaging (rename, PG14+ modernization, build system) | Core foundation |
| Sprint 2 | pg_cron lifecycle (`start`/`stop`/`status`) | New code |
| Sprint 3 | Modern API (`send`/`receive`/`ack`/`nack`, DLQ, delayed) | High-value |
| Sprint 4 | Observability (metrics, health, OTel) | Operational |
| Sprint 5 | Client libraries: Python + Go v1 | Developer experience |
| Sprint 6 | Testing, benchmarks, docs, CI/CD (PG 14-18) | Production-ready |

**v2 (planned):** Node.js and Ruby client libraries, `peek()`, push-based
OTLP exporter, trace propagation in client SDKs.

## Contributing

See [SPECx.md](blueprints/SPECx.md) for the specification and implementation
plan. All new code follows red/green TDD -- write the failing test first, then
the implementation.

## License

Apache-2.0. See [LICENSE](LICENSE).

PgQue includes code derived from [PgQ](https://github.com/pgq/pgq) (ISC
license, Marko Kreen / Skype Technologies OU). See [NOTICE](NOTICE).
