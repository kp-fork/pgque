# PgQue Partition Keys — Spec

- **Version:** v0.3 (draft)
- **Status:** review rounds 1 + 2 applied. Core model verified sound against the
  engine (G1 ordering confirmed true; G2 lock confirmed real). Remaining fixes
  from round 2 folded in. See §15 changelog and `decisions.md`.
- **Slug:** partition-keys
- **Scope:** consumer-side ordered, parallel consumption by partition key.
  Producer-side idempotency/dedup is a *separate* spec (deferred — §12).

---

## 1. Goal

Add a **partition key** to PgQue so that, within one queue, events sharing a key
are consumed **in order by a single worker at a time**, while events with
different keys are consumed **in parallel** — the log-native ("Kafka partition")
model: order *within* a key, parallelism *across* keys.

## 2. The guarantee (precise, testable)

- **G1 — per-key affinity + FIFO.** For a queue whose events carry a partition
  key, and a fixed slot count `N`, every event of key `K` maps to exactly one
  slot `slot(K) = (hashtextextended(K, 0) % N + N) % N` (the `+N` normalization
  is mandatory — `hashtextextended` returns `bigint` and `%` of a negative value
  is negative; round 2). Within that slot, non-retried events of `K` are
  delivered in non-decreasing `ev_id` order, to **no other slot**.
  - *Intra-batch order* is guaranteed by the engine's `order by 1` (= `ev_id`)
    in `batch_event_sql` (`pgque.sql:440`), preserved through the
    `get_batch_cursor` filter wrap (`pgque.sql:2277`). *Cross-batch order* for a
    slot follows from sequential consumption of one subscription whose cursor
    advances monotonically — **not** from `order by 1` (which is per-batch).
- **G2 — single in-flight processor per key.** At any instant at most one worker
  holds an unacked event for `K`. Enforced by the per-subscription receive lock
  (`next_batch_custom` … `for update of s`, `pgque.sql:5761`), the same lock the
  #97/#125 double-delivery guard relies on (`tests/two_session_receive_lock.sh`).
- **G3 — failure boundary.** Under **`pause`**, no later event of `K` is
  delivered until `K`'s failed head event is acked or dead-lettered; other keys
  unaffected. Under **`skip`** (v0.1 default), later events of `K` MAY be
  delivered before the failure resolves — only *at-least-once* holds.
  - **Engine fact:** a retried event keeps its `ev_id` but gets a new `ev_txid`
    (re-injected by `maint_retry_events` → `insert_event_raw`, `pgque.sql:859`),
    so it reappears in a *later* batch. Thus G1's `ev_id` monotonicity holds only
    between non-retried events; across a retry the only guarantee is G3's pause
    boundary. The retried event re-routes to the **same slot** because
    `ev_extra1` is preserved through the retry path (verified `pgque.sql:2376`,
    `:861`).

## 3. Why it's needed

PgQue is an **ordered, immutable log**, not a job queue — workloads need
**per-entity ordering without global ordering**. Motivating case (a multi-tenant
storage service evaluating PgQue to replace pg-boss): millions of file-lifecycle
events that **must be ordered per tenant** but need **no ordering across
tenants**. One in-order consumer can't keep up; naive multi-worker consumption
breaks per-tenant order.

## 4. Scope and ICP

**In scope (v0.1 implementation):**
- Partition key on a `send()`-sourced event.
- N independent **slot consumers**, each filtering the stream to its hash class
  (§6). Stable `(hashtextextended(key,0) % N + N) % N` affinity.
- G1 + G2 always; **`skip` failure policy as the v0.1 default** (sound, stateless).

**Deferred to v0.2 implementation (specified here, not built first):**
- **`pause` failure policy** (G3 strict) — uses a compact blocked-key marker
  (§7 D5, §8). Carries the crash/rotation risks (R1, R7); ships after `skip`.

**Out of scope:** producer idempotency (separate spec, §12); dynamic
rebalancing / elastic `N` (§7 D3, R4); **trigger-sourced queues**
(`jsontriga`/`logutriga`/`sqltriga` store the table name in `ev_extra1` — §7 D1,
R5); cross-queue / cascaded partitioning; automatic hot-partition mitigation.

**ICP:** multi-tenant SaaS on managed Postgres with a high-volume per-entity
event stream (entity = partition key: tenant, user, document, device).

## 5. End-to-end workflow

```
producer:  pgque.send('files', 'default', payload, partition_key => tenant_id)
                       │  key → ev_extra1 (send-sourced queues only, v0.1)
                       ▼
engine:    append-only event tables · global ev_id/ev_txid order   (UNCHANGED)
                       │  full stream
                       ▼
consumers: N slot consumers, each an INDEPENDENT subscription with its own cursor;
           slot k reads the whole stream and server-side-filters to its hash class
```

## 6. Architecture

The v0.1 mechanism is **N independent slot consumers**, *not* a modification of
cooperative consumers (round 1 B1: coop hands each member a disjoint tick window
and cannot fan one batch to N hash-filtered slots without dropping events on the
shared cursor advance; confirmed `cooperative_consumers.sql`, `for update skip
locked` victim-steal at `pgque.sql:6262`).

<!-- architecture:begin -->
```
 producers │ send(queue, type, payload, partition_key => K) → ev_extra1
           ▼
 ┌──────────────────────────────────────────────────────────┐
 │ ENGINE · sacred — UNCHANGED                                │
 │ append-only tables · global ev_id/ev_txid · rotation      │
 │ next_batch / get_batch_cursor(i_extra_where) / order by 1 │
 └───────┬───────────────┬───────────────┬──────────────────┘
         │ full stream    │ full stream    │ full stream
         ▼                ▼                ▼
   slot 0 (sub#0/N)  slot 1 (sub#1/N)  slot N-1
   own cursor        own cursor        own cursor
   filter h%N=0      filter h%N=1      filter h%N=N-1
   one worker        one worker        one worker
```
<!-- architecture:end -->

**Filtering without touching the engine.** Each slot's receive reuses the
existing admin-only `pgque.get_batch_cursor(…, i_extra_where)` hook
(`pgque.sql:2229`), injecting `and (hashtextextended(ev_extra1,0) % N + N) % N =
k`. The fragment is assembled **only** from the validated integers `N`, `k`
(§8) — never a caller string. `batch_event_sql`, `next_batch`, rotation are
**not modified**; `get_batch_cursor`'s `order by 1` re-wrap preserves G1.

**Trust boundary (round 2).** `get_batch_cursor`'s `i_extra_where` is a
documented *trusted-SQL sink*, revoked from `public/pgque_reader/pgque_writer`
and granted to `pgque_admin` only (`pgque.sql:2221`, `:4852`). Therefore
`receive_partitioned` and `subscribe_slot` are **`SECURITY DEFINER`, owned by the
installer** (which holds the admin grant), exactly like `receive`/`nack`
(`receive.sql`). The reader role gets EXECUTE on the *wrappers*, never on
`get_batch_cursor`. Safety rests on the wrapper validating and integer-casting
`N`,`k` and interpolating no caller-supplied value — **not** on "the string
contains only integers."

**Each slot is its own subscription** → its own cursor (`sub_last_tick`) and
`sub_id` → no cross-slot data loss, and retry/DLQ rows are naturally slot-scoped
(`ev_owner = sub_id`, `pgque.sql:2374`).

**Known cost — read amplification.** Every event is scanned by all `N` slot
cursors (each discards `(N-1)/N` after the hash filter; the filter is applied
*after* the engine materializes the per-slot window, `pgque.sql:2277`, so it
reduces returned rows, not scan work). Steady state ≈ **N×**; **up to ~2N×**
during a rotation-overlap window (the engine's multi-table `union all`); a
**stalled slot scans an ever-widening window** each poll. Documented;
single-reader/dispatch optimization noted as future work (R6).

## 7. Decisions

| ID | Decision | Choice (v0.3) | Rationale / round-2 change |
|----|----------|---------------|----------------------------|
| D1 | Where the key lives | `ev_extra1`, **`send()`-sourced queues only** | Triggers use `ev_extra1` for the table name. |
| D2 | Failure policy | `skip` default (v0.1); `pause` ships v0.2 | `pause` needs the blocked-key marker (D5). |
| D3 | Elasticity & N | Fixed `N`, **persisted in a `pgque.partition_consumer(queue, consumer, n)` row**; a worker registering a different `n` is **rejected** | "Fixed N" is an enforced invariant, not a convention. N lives outside the slot names (which encode it) so a *new* slot can be validated. |
| D4 | Assignment | `(hashtextextended(key, 0) % N + N) % N` | `hashtextextended` is the stable, documented hash; the `+N` normalizes the sign (round 2). |
| D5 | State budget | **Happy path & `skip`: no state.** `pause`: a compact `pgque.partition_block(sub_id, partition_key, head_ev_id)` marker, written on first failure, cleared on ack-or-DLQ | Round 2 (B-R2-2) proved `retry_queue` is transient (deleted by `maint_retry_events`, `pgque.sql:863`) so it can't be the durable blocked-set. The marker is O(*concurrently-failing keys*) — proportional to failures, **not** throughput, so it is not pg-boss-style per-event churn. Honestly reopens "no new table," scoped to `pause`. |
| D6 | Producer signature | `send(queue, type, payload, partition_key => text)` (new 4-arg overload) | Avoids collision with `send(queue, type, payload)`. |
| D7 | Slot identity & single-owner | slot = consumer `"<consumer>#k/N"`; G2 via the per-subscription receive lock. `receive_partitioned`/`subscribe_slot` are `SECURITY DEFINER` (§6) | Defines what a slot is and what makes G2 true and reader-callable. |

## 8. Implementation details

- **Producer:** `pgque.send(queue, type, payload, partition_key text default
  null)` → `insert_event(queue, type, payload, ev_extra1 => partition_key, …)`.
  `SECURITY DEFINER set search_path = pgque, pg_catalog`; revoke from public,
  grant `pgque_writer`.
- **Slot registration:** `pgque.subscribe_slot(queue text, consumer text, k int,
  n int)` — validates `n >= 1 and 0 <= k < n` (raises otherwise), upserts the
  persisted `n` for `(queue, consumer)` and **rejects a changed `n`** (D3),
  registers subscription `"<consumer>#k/n"`. Idempotent for the same `(k,n)`.
- **Partitioned receive:** `pgque.receive_partitioned(queue, consumer, k int, n
  int, …)` — after validating/casting `k,n` to int, calls `next_batch` +
  `get_batch_cursor(…, i_extra_where => format('and (hashtextextended(ev_extra1,0)
  %% %s + %s) %% %s = %s', n, n, n, k))`. Under `pause`, the wrapper also
  withholds events whose key has an open `partition_block` row for this `sub_id`
  with `head_ev_id < ev_id`. `SECURITY DEFINER` (§6); granted `pgque_reader`.
- **`pause` lifecycle (v0.2):** on nack of `K#i`, upsert
  `partition_block(sub_id, K, head_ev_id => ev_id)`. The row is **durable**, so
  it survives a crash with no reconstruction. Clear it when `K#i` is acked
  (success after retry) **or** dead-lettered — unblock predicate: ack, OR a
  `dead_letter` row exists for `(dl_consumer_id = this slot, ev_id = K#i)`
  (`event_dead`, `dlq.sql`). So a poison key cannot wedge past `max_retries`.
- **`pause` must not hold the batch open** (R7): it acks the batch and tracks
  blocked keys via `partition_block`, so the slot's cursor keeps advancing past
  *non-blocked* keys and does not pin rotation.
- **Teardown:** `pgque.unsubscribe_slot(queue, consumer, k, n)` removes the slot
  subscription; full-consumer teardown removes all N + the
  `partition_consumer`/`partition_block` rows. **Caveat:** `unregister_consumer`
  cascades `dead_letter` (`on delete cascade`), so tearing down a slot drops that
  slot's DLQ audit — documented.
- **Grants:** producer overload → `pgque_writer`; `subscribe_slot` /
  `unsubscribe_slot` / `receive_partitioned` → `pgque_reader`. Deny-by-default
  re-applied. `get_batch_cursor` stays revoked from all app roles.

## 9. Tests plan (red/green TDD)

CI matrix PG 14–18. Write the failing test first.

- **T-G1a (affinity, stable):** assert the literal `(hashtextextended(K,0)%N+N)%N`
  on **every** CI PG version, pinning one concrete `(K, expected)` pair so a hash
  drift is caught even if all versions move together. *(red first)*
- **T-G1b (per-key FIFO):** interleave A,B,A,A,B; assert each key in `ev_id` order
  across batches, no key on two slots. *(No existing test guards intra-batch
  ev_id order — this is new and load-bearing.)*
- **T-retry-affinity (new, round 2):** nack a keyed event; run
  `maint_retry_events()` + `force_next_tick` + `ticker()`; assert it redelivers
  to the **same** slot `k` and no other. *(The core correctness property under
  retry.)*
- **T-G2-block:** two workers on the **same** slot → second blocks on the
  subscription lock (mirror `tests/two_session_receive_lock.sh`).
- **T-G2-parallel:** two workers on **different** slots → neither blocks (the
  parallelism half).
- **T-no-drop:** keys spanning all slots in one tick window; run all N slots;
  union delivered = all events, zero loss.
- **T-security (new, round 2):** a bare `pgque_reader` can call
  `receive_partitioned`/`subscribe_slot` end-to-end; and `pgque_reader` **cannot**
  call `get_batch_cursor` directly (mirror
  `tests/test_security_get_batch_cursor.sql`); `receive_partitioned` rejects a
  non-integer/out-of-range `n`/`k`.
- **T-G3-pause (order-after-retry):** A#2 nacked (`pause`); drive
  `maint_retry_events()` + tick; assert A#3 withheld until A#2 acked-or-DLQ'd; B
  unaffected.
- **T-DLQ-unblock:** A#2 exhausts retries → `dead_letter`; assert A#3 then
  proceeds.
- **T-slot-crash:** worker holding A#2 (and its `partition_block` row) dies; a
  new worker takes slot k; drive maint+tick; assert A#2 redelivered before A#3
  and only to slot k. *(Crash specifically in the post-`maint` window where the
  `retry_queue` row is already gone — the round-2 hole.)*
- **T-G3-skip (reorder boundary):** with `skip`, assert the exact permitted
  reorder after A#2 fails.
- **T-N-invariant (new, round 2):** `subscribe_slot(…,k,n)` is idempotent;
  `subscribe_slot(…,k,n2≠n)` raises.
- **T-empty-slot / T-hot-key:** an empty slot doesn't wedge others; a hot key
  saturates one slot while others drain.
- **T-no-bloat (happy path):** all-ack processing of M events adds zero
  `retry_queue`/`dead_letter`/`partition_block` rows and no per-event
  UPDATE/DELETE. (Failure path legitimately writes — out of scope here.)
- **T-engine-untouched:** `pg_get_functiondef` of `batch_event_sql`,
  `next_batch_custom`, **and `get_batch_cursor`** (round 2 — the slot model
  depends on its `order by 1` re-wrap) byte-identical to baseline.
- **T-idempotent-install:** re-running `pgque.sql` re-creates the partition
  functions/tables cleanly.

## 10. Risks and open questions

- **R1 — `pause` crash safety: resolved by D5's durable `partition_block`
  marker** (round 2 B-R2-2). Test: T-slot-crash with the crash in the
  post-`maint` window.
- **R2 — read amplification:** N× steady, ~2N× during rotation overlap,
  unbounded-width for a stalled slot (§6). Benchmark the stalled case, not just
  uniform N.
- **R3 — hot partitions:** one hot key saturates its slot; documented only.
- **R4 — changing N:** an enforced invariant (D3); true rebalancing is future.
- **R5 — `ev_extra1` semantics:** restricted to `send()`-sourced queues; a
  dedicated partition-key column is future work.
- **R6 — single-reader/dispatch optimization** to remove read amplification;
  adds a hop/state, out of v0.1's budget.
- **R7 — rotation wedge (round 2):** rotation waits for `min(sub_last_tick)` over
  ALL subscriptions (`pgque.sql:910`); N slots lower that floor to the slowest
  slot, and a wedged `pause` slot could pin rotation for the whole queue →
  unbounded data growth. Mitigated by §8 ("`pause` does not hold the batch open";
  cursor advances past non-blocked keys) + an alert on per-slot staleness.

## 11. (reserved)

## 12. Relationship to producer idempotency (deferred sibling)

Producer-side dedup is a **TTL window** (SQS/NATS model), append-only, GC'd by
rotation — a separate spec. In a log, "processed" is a per-consumer fact the
producer cannot see, so dedup is a producer-side time window while
ordering/serialization is this consumer-side feature. Rationale and prior art:
`blueprints/idempotency/DESIGN.md`.

## 13. Team of veteran experts (review panel)

- **Lead:** drafts/revises (this document).
- **Reviewer A — ops/security:** rounds 1 + 2 applied (security trust boundary,
  blocked-set durability, rotation wedge, modulo sign).
- **Reviewer B — QA/testability:** rounds 1 + 2 applied (confirmed G1 ordering +
  G2 lock; grant/DEFINER wiring; retry-affinity, security, N-invariant tests).

## 14. Sprint plan

1. **S1 — producer + key plumbing:** `send(…, partition_key =>)` on
   send-sourced queues. Tests T-G1a, T-no-bloat(happy), T-idempotent-install.
2. **S2 — slot consumers (`skip` default):** `subscribe_slot` (persisted N, D3),
   `receive_partitioned` via `get_batch_cursor` `extra_where` (`SECURITY
   DEFINER`, D7), teardown. Tests T-G1b, T-retry-affinity, T-G2-block,
   T-G2-parallel, T-no-drop, T-security, T-N-invariant, T-G3-skip,
   T-engine-untouched.
3. **S3 — `pause` policy (v0.2):** `partition_block` marker; DLQ-unblock; "no held
   batch" (R7). Tests T-G3-pause, T-DLQ-unblock, T-slot-crash.
4. **S4 — docs + benchmark:** throughput vs N; read-amp (steady, rotation,
   stalled); per-tenant order under load.

## 15. Changelog

- **v0.3 (draft):** review round 2 applied. **Confirmed G1 ordering is true**
  (engine `order by 1`, preserved through `get_batch_cursor`) and the G2 lock is
  real/tested. Fixed: (security) `receive_partitioned`/`subscribe_slot` are
  `SECURITY DEFINER` over the admin-only `get_batch_cursor`, with integer
  validation and the real trust-boundary argument (B-R2-1); (correctness)
  `pause` blocked-set moved from the transient `retry_queue` to a durable compact
  `partition_block` marker (B-R2-2 / D5); (bug) modulo normalized
  `(h%N+N)%N` (D4); added R7 rotation-wedge + "no held batch"; specified N
  persistence + teardown + DLQ-cascade caveat (D3); explicit DLQ-unblock
  predicate. Added tests: T-retry-affinity, T-security, T-N-invariant, split
  T-G2 block/parallel, `get_batch_cursor` in T-engine-untouched, pinned hash
  pair. Round-2 decisions in `decisions.md`.
- **v0.2 (draft):** review round 1 — re-grounded the mechanism to N independent
  slot subscriptions; restated G1/G2/G3; corrected retry rationale; `skip`
  default. (Full detail in `decisions.md`.)
- **v0.1 (draft):** initial single-pass SamoSpec-format draft.
