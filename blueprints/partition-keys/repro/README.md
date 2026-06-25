# Partition-keys reproduction

A runnable spike for the two-tier partition-keys design
(`blueprints/partition-keys/SPEC.md`). It installs pgque on a throwaway
Postgres, drives both workloads with concurrent workers, **measures
throughput**, and **checks the guarantees empirically** — so we can test the
design instead of arguing about it.

Both tiers are built entirely on existing engine primitives. There are no
engine changes here; everything consumer-side is a thin recipe in `schema.sql`.

| Tier | Workload | Mechanism (existing primitives) | Guarantee checked |
|------|----------|----------------------------------|-------------------|
| **A** | migrations — one job per tenant | cooperative consumers + per-key `pg_try_advisory_xact_lock` | **G2** mutual exclusion; idempotency collapse |
| **B** | lifecycle events — ordered per tenant | N slot subscriptions filtering via `get_batch_cursor` `extra_where` | **G1** per-key FIFO + single-slot affinity; read amplification |

## Run it on a fresh Linux VM

```bash
# Debian/Ubuntu VM, from inside the pgque repo:
sudo bash blueprints/partition-keys/repro/run.sh
```

`run.sh` installs Postgres + `python3-psycopg2`, creates `pgque_repro`,
installs `sql/pgque.sql` + `schema.sql`, then runs both tiers and prints a
report ending in `PASS`/`FAIL` lines for each invariant.

### Run a single tier / change scale

```bash
# bigger Tier A, 16 workers, heavier simulated work:
A_WORKERS=16 A_TENANTS=5000 A_DUPS=6 A_WORK_MS=5 bash blueprints/partition-keys/repro/run.sh

# or call the driver directly (after setup.sh):
sudo -u postgres PGQUE_DSN=dbname=pgque_repro \
  python3 blueprints/partition-keys/repro/driver.py \
  --tier b --tenants 1000 --events-per-tenant 30 --slots 16
```

Knobs (env for `run.sh`, flags for `driver.py`):

- Tier A: `A_TENANTS` / `--tenants`, `A_DUPS` / `--dups` (duplicate events per
  tenant — exercises contention + idempotency), `A_WORKERS` / `--workers`,
  `A_WORK_MS` / `--work-ms` (simulated work while holding a key).
- Tier B: `B_TENANTS` / `--tenants`, `B_EPT` / `--events-per-tenant`,
  `B_SLOTS` / `--slots` (= N = worker count, static assignment).
- Both: `CHUNK` / `--chunk` — events per tick (more ticks → coop spreads
  Tier A across more workers).

## What each report tells you

**Tier A**
- `jobs RUN` vs `ack-dropped` — duplicates and contended events collapse to one
  run per tenant (the idempotency ask, solved consumer-side).
- `per-worker ran` — the cooperative consumer spread the load; no leader, no
  assignment.
- Invariants: no two workers ran the same key with overlapping windows (G2);
  every tenant migrated exactly once (`runs == 1`).

**Tier B**
- `consume throughput` and `read amplification` — measured `scanned/delivered`,
  which should sit near `N` (every slot scans the full window). This is the
  cost of strict ordering, quantified.
- Invariants: each key delivered to exactly one slot (affinity) and in
  non-decreasing `ev_id` order (FIFO); nothing lost or duplicated.

## Other distros / existing Postgres

`setup.sh` is Debian/Ubuntu-flavored. If you already have Postgres + psycopg2,
skip it and point the driver at your DB:

```bash
psql -d pgque_repro -f sql/pgque.sql
psql -d pgque_repro -f blueprints/partition-keys/repro/schema.sql
PGQUE_DSN='host=/var/run/postgresql dbname=pgque_repro' \
  python3 blueprints/partition-keys/repro/driver.py --tier a
```

## Caveats (it's a spike)

- One-shot: produce-then-drain, not a steady-state stream. Throughput numbers
  are indicative, not a tuned benchmark.
- Tier A processes a whole cooperative batch in one transaction (the per-key
  advisory lock is `xact`-scoped), so the visible processing window equals the
  batch, not a single event — fine for checking exclusion, not for latency.
- Tier B uses static `worker k → slot k` assignment; the dynamic claim variants
  (advisory / `SKIP LOCKED`) from SPEC §15 are not exercised here.
