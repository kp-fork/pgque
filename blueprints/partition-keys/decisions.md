# Partition Keys — decisions log

Accepted / rejected / deferred choices, tracked across review rounds.

## Review round 1 (Reviewer A ops/security · Reviewer B QA/testability)

### Accepted (changed the spec)

- **A1 — Drop the cooperative-consumer distribution model.** Both reviewers
  proved coop hands each member a *disjoint tick window*, not a hash-filtered
  shared batch; a filter overlay would drop other slots' events on cursor
  advance. → v0.2 uses **N independent slot subscriptions**, each filtering the
  full stream via `get_batch_cursor` `extra_where`. (SPEC §6)
- **A2 — Correct the retry rationale.** `event_retry` preserves `ev_id` and
  changes `ev_txid` (event reappears in a later window); the original "later
  ev_id" claim was wrong. → guarantee restated as G1/G2/G3 with an explicit
  engine note. (SPEC §2)
- **A3 — Resolve D2-vs-"no state".** `pause` derives its blocked-key set from the
  engine's existing `retry_queue`/`dead_letter`, scoped per slot by `sub_id`
  (each slot is its own subscription); no new mutable table. (SPEC §7 D5, §8)
- **A4 — DLQ must unblock.** A paused key releases when its head event is acked
  *or* dead-lettered, so a poison event cannot wedge a tenant past `max_retries`.
  (SPEC §8; test T-DLQ-unblock)
- **A5 — `send` signature.** Use a new 4-arg `send(queue, type, payload,
  partition_key =>)`; a 3-arg `send(queue, key, payload)` collides with the
  existing `send(queue, type, payload)`. (SPEC §7 D6)
- **A6 — `hashtextextended(key, 0)`** instead of `hashtext()` (unstable across PG
  majors → affinity would break on upgrade). (SPEC §7 D4)
- **A7 — `ev_extra1` restricted to `send()`-sourced queues** (triggers store the
  table name there). (SPEC §7 D1)
- **A8 — Fixed N as an enforced invariant**: persisted per (queue, consumer); a
  mismatched-N worker is rejected, not silently misrouting. (SPEC §7 D3)
- **A9 — Define "slot" and single-owner**: slot = named consumer `"<c>#k/N"`;
  G2 enforced by the existing per-consumer receive lock. (SPEC §7 D7)
- **A10 — Test corrections**: `T-engine-untouched` asserts `pg_get_functiondef`
  (not generated SQL); `T-no-bloat` scoped to the happy path; added T-no-drop,
  order-after-retry, DLQ-unblock, slot-crash, empty/hot-key, cross-version
  affinity. (SPEC §9)

### Deferred

- **`pause` policy implementation** ships after `skip` (sound + simple first);
  `skip` is the v0.1 default. `pause` is fully specified. (SPEC §4, §7 D2)
- **Read-amplification optimization** (single-reader/dispatch) — R6; adds a
  hop/state, out of v0.1's no-state budget.
- **Trigger-sourced queues**, **dynamic N / rebalancing**, **hot-partition
  mitigation** — out of scope, documented.

### Rejected

- **Lease table / advisory-lock-per-event** for serialization — reintroduces the
  per-event churn PgQue exists to avoid (carried over from the superseded
  `IDEMPOTENCY_AND_PARTITIONS.md`, now removed).
- **Modifying `batch_event_sql`** to push partitioning into the engine — violates
  the sacred-engine rule; the `extra_where` hook achieves filtering without it.

## Review round 2 (both personas, verified against the engine)

### Confirmed sound (no change needed)
- **G1 `ev_id` ordering is true.** `batch_event_sql` emits `order by 1`
  (`pgque.sql:440`); `get_batch_cursor` re-wraps the filtered stream with
  `order by 1` (`pgque.sql:2277`) → per-key order survives the filter, no
  consumer sort. (Reviewer B headline.)
- **G2 single-owner lock is real and tested** — `next_batch_custom … for update
  of s` (`pgque.sql:5761`), guarded by `two_session_receive_lock.sh`.
- **Retry affinity holds** — `ev_extra1` preserved through `event_retry` /
  `maint_retry_events`, so a retried event re-routes to the same slot.
- **Coop genuinely hands disjoint windows** (`for update skip locked`,
  `pgque.sql:6262`) — confirms round-1 A1.

### Accepted (changed the spec → v0.3)
- **B-R2-2 — `pause` blocked-set must be durable.** `retry_queue` is transient
  (`maint_retry_events` deletes the row on re-injection, `pgque.sql:863`),
  leaving a crash hole that violates G3. → durable
  `partition_block(sub_id, partition_key, head_ev_id)` marker, O(failing keys),
  cleared on ack-or-DLQ. Honestly reopens "no new table," scoped to `pause`.
  (D5, §8, R1)
- **B-R2-1 — security trust boundary.** `get_batch_cursor.extra_where` is an
  admin-only trusted-SQL sink (`pgque.sql:2221`, `:4852`). → `receive_partitioned`
  / `subscribe_slot` are `SECURITY DEFINER` installer-owned; integers `n,k`
  validated + cast; no caller string interpolated. Reframed the "injection-safe"
  prose. (§6, §8, D7; test T-security)
- **Negative modulo bug** — `hashtextextended` returns `bigint`; bare `% N` can
  be negative → `(h % N + N) % N`. (D4, §6, §8)
- **R7 rotation wedge** — N slots lower the rotation floor to the slowest slot; a
  wedged `pause` slot could pin rotation for the whole queue. → `pause` must not
  hold the batch open; cursor advances past non-blocked keys. (R7, §8)
- **N persistence + teardown** — N persisted in `partition_consumer`;
  `unsubscribe_slot` + DLQ-cascade caveat. (D3, §8)
- **Tests** — added T-retry-affinity, T-security, T-N-invariant; split T-G2 into
  block/parallel; added `get_batch_cursor` to T-engine-untouched; pinned a
  concrete hash pair. (§9)

### Round-1 closure scorecard (both reviewers)
- B1 (coop model) ✅ closed · B2 (retry rationale) ✅ closed · B3 (crash-derive
  blocked set) ♻️ reopened in v0.2, now ✅ closed via durable marker (D5) ·
  B4 (D2-vs-state / send sig) ✅ closed · B5 (DLQ-unblock / slot definition)
  ✅ closed; spawned B-R2-1 (now fixed) · B6 ✅ closed.

## Still open (for round 3, if run)
- Bench numbers for read amplification (steady N× vs ~2N× rotation vs stalled
  slot) to decide if R6 (single-reader/dispatch) is needed in v0.1.
- Exact `partition_block` withhold predicate wording in `receive_partitioned`
  (server-side `not exists` vs worker-side filter).
