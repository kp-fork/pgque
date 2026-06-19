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

## Open (for round 2)

- Exact `retry_queue` predicate for `pause` blocked-set reconstruction at slot
  start (R1).
- Whether read amplification at target throughput justifies R6 in v0.1.
