#!/usr/bin/env python3
"""Partition-keys reproduction driver.

Produces a keyed workload, drains it with N concurrent workers, measures
throughput, and checks the design's guarantees empirically against a real
pgque install.

  Tier A (mutual exclusion):  cooperative consumers + per-key advisory lock.
  Tier B (ordered per key):   N hash-routed slot subscriptions.

Usage:
  python3 driver.py --tier a --tenants 2000 --dups 4 --workers 8 --work-ms 3
  python3 driver.py --tier b --tenants 500 --events-per-tenant 20 --slots 8

Connection: env PGQUE_DSN (default "dbname=pgque_repro").
"""
import argparse
import os
import random
import sys
import threading
import time

try:
    import psycopg2
except ImportError:
    sys.exit("psycopg2 not installed: sudo apt-get install -y python3-psycopg2")

DSN = os.environ.get("PGQUE_DSN", "dbname=pgque_repro")
SEED = 1234


def connect():
    c = psycopg2.connect(DSN)
    c.autocommit = True
    return c


def reset_queue(cur, queue, force_drop=True):
    if force_drop:
        try:
            cur.execute("select pgque.drop_queue(%s, true)", (queue,))
        except psycopg2.errors.RaiseException:
            pass  # queue did not exist yet
    cur.execute("select pgque.create_queue(%s)", (queue,))


def tick(cur, queue):
    """Make produced events visible to consumers (separate txn from inserts)."""
    cur.execute("select pgque.force_tick(%s)", (queue,))
    cur.execute("select pgque.ticker(%s)", (queue,))


# ---------------------------------------------------------------------------
# Tier A — mutual exclusion (migration-style workload)
# ---------------------------------------------------------------------------
def run_tier_a(args):
    queue, main = "migrations", "mig"
    rnd = random.Random(SEED)
    c = connect()
    cur = c.cursor()
    cur.execute("select demo.reset()")
    reset_queue(cur, queue)
    # register the cooperative subconsumers (one per worker)
    workers = [f"w{i}" for i in range(args.workers)]
    for w in workers:
        cur.execute("select pgque.register_subconsumer(%s, %s, %s)", (queue, main, w))

    # produce: each tenant emits `dups` identical "migrate" events; the whole
    # set is shuffled so duplicates of one tenant land in different tick windows
    # (and thus reach different workers) — the contention we want to exercise.
    events = []
    for t in range(args.tenants):
        for _ in range(args.dups):
            events.append(f"tenant-{t}")
    rnd.shuffle(events)
    total = len(events)

    t0 = time.time()
    pending = 0
    for key in events:
        cur.execute("select demo.produce(%s, %s, %s, %s)",
                    (queue, "migrate", '{"op":"ensure_latest"}', key))
        pending += 1
        if pending >= args.chunk:
            tick(cur, queue)
            pending = 0
    if pending:
        tick(cur, queue)
    produce_s = time.time() - t0
    c.close()

    # drain with N workers
    stats = {w: {"got": 0, "ran": 0, "dropped": 0} for w in workers}
    lock = threading.Lock()

    def work(w):
        wc = connect()
        wcur = wc.cursor()
        empty = 0
        local = {"got": 0, "ran": 0, "dropped": 0}
        while empty < 2:
            wcur.execute("select got, ran, dropped from demo.tier_a_consume(%s,%s,%s,%s,%s)",
                         (queue, main, w, args.max_batch, args.work_ms))
            got, ran, dropped = wcur.fetchone()
            local["got"] += got
            local["ran"] += ran
            local["dropped"] += dropped
            empty = empty + 1 if got == 0 else 0
            if got == 0:
                time.sleep(0.01)
        wc.close()
        with lock:
            stats[w] = local

    t1 = time.time()
    run_workers(work, workers)
    drain_s = time.time() - t1

    # ---- report + invariants ----
    c = connect()
    cur = c.cursor()
    ran = sum(s["ran"] for s in stats.values())
    got = sum(s["got"] for s in stats.values())
    dropped = sum(s["dropped"] for s in stats.values())

    cur.execute("select count(*) from demo.tenant_migrated")
    migrated = cur.fetchone()[0]
    cur.execute("select count(*) from demo.tenant_migrated where runs > 1")
    double_run = cur.fetchone()[0]
    # overlapping processing windows for the same key across different workers
    cur.execute("""
        select count(*) from demo.mutex_log a join demo.mutex_log b
          on a.part_key = b.part_key and a.worker <> b.worker and a.id < b.id
         and a.started_at < b.ended_at and b.started_at < a.ended_at""")
    overlaps = cur.fetchone()[0]
    cur.execute("select count(*) from demo.mutex_log where ended_at is null")
    unfinished = cur.fetchone()[0]
    c.close()

    print(f"  produced events         : {total}  ({args.tenants} tenants x {args.dups} dups)")
    print(f"  produce time            : {produce_s:6.2f}s")
    print(f"  drain time              : {drain_s:6.2f}s   ({args.workers} workers, work-ms={args.work_ms})")
    print(f"  consume throughput      : {got/drain_s:,.0f} events/s")
    print(f"  jobs RUN (migrations)   : {ran}")
    print(f"  ack-dropped (dup/contend): {dropped}")
    per_worker = ", ".join("{}:{}".format(w, stats[w]["ran"]) for w in workers)
    print(f"  per-worker ran          : {{ {per_worker} }}")
    print("  ---- invariants ----")
    ok = True
    ok &= check("G2 mutual exclusion: no overlapping runs per key", overlaps == 0, f"{overlaps} overlaps")
    ok &= check("no double-run per tenant (runs==1)", double_run == 0, f"{double_run} tenants ran >1x")
    ok &= check("idempotency collapse: 1 run per tenant", ran == args.tenants and migrated == args.tenants,
                f"ran={ran}, migrated={migrated}, expected={args.tenants}")
    ok &= check("all processing windows closed", unfinished == 0, f"{unfinished} unfinished")
    return ok


# ---------------------------------------------------------------------------
# Tier B — ordered per key (lifecycle workload)
# ---------------------------------------------------------------------------
def run_tier_b(args):
    queue = "lifecycle"
    n = args.slots
    c = connect()
    cur = c.cursor()
    cur.execute("select demo.reset()")
    reset_queue(cur, queue)
    slots = [f"life_slot_{k}" for k in range(n)]
    for s in slots:
        cur.execute("select pgque.register_consumer(%s, %s)", (queue, s))

    # produce ordered lifecycle events, interleaved across tenants so each tick
    # window holds a mix of keys. Per-tenant emission order is preserved (round j
    # emits each tenant's j-th event), so ev_id is monotonic per key.
    types = ["FileCreated", "FileOverwritten", "FileDeleted"]
    total = 0
    t0 = time.time()
    pending = 0
    for j in range(args.events_per_tenant):
        etype = types[0] if j == 0 else (types[2] if j == args.events_per_tenant - 1 else types[1])
        for t in range(args.tenants):
            cur.execute("select demo.produce(%s, %s, %s, %s)",
                        (queue, etype, '{"seq":%d}' % j, f"tenant-{t}"))
            total += 1
            pending += 1
            if pending >= args.chunk:
                tick(cur, queue)
                pending = 0
    if pending:
        tick(cur, queue)
    produce_s = time.time() - t0
    c.close()

    # drain: static assignment — worker k owns slot k
    agg = {"scanned": 0, "delivered": 0}
    lock = threading.Lock()

    def work(k):
        wc = connect()
        wcur = wc.cursor()
        empty = 0
        scanned = delivered = 0
        while empty < 2:
            wcur.execute("select scanned, delivered from demo.tier_b_consume(%s,%s,%s,%s,%s)",
                         (queue, f"life_slot_{k}", k, n, args.max_batch))
            sc, de = wcur.fetchone()
            scanned += sc
            delivered += de
            # next_batch returned null -> sc==0 and de==0
            empty = empty + 1 if (sc == 0) else 0
            if sc == 0:
                time.sleep(0.01)
        wc.close()
        with lock:
            agg["scanned"] += scanned
            agg["delivered"] += delivered

    t1 = time.time()
    run_workers(work, list(range(n)))
    drain_s = time.time() - t1

    # ---- report + invariants ----
    c = connect()
    cur = c.cursor()
    cur.execute("select count(*) from demo.consume_log")
    consumed = cur.fetchone()[0]
    cur.execute("select count(distinct msg_id) from demo.consume_log")
    distinct_msgs = cur.fetchone()[0]
    # keys delivered to more than one slot (affinity break)
    cur.execute("""
        select count(*) from (
          select part_key from demo.consume_log group by part_key
          having count(distinct slot) > 1) z""")
    multislot = cur.fetchone()[0]
    # per-key out-of-order delivery
    cur.execute("""
        select count(*) from (
          select msg_id, lag(msg_id) over (partition by part_key order by seq) as prev
          from demo.consume_log) t
        where prev is not null and msg_id < prev""")
    outoforder = cur.fetchone()[0]
    c.close()

    amp = (agg["scanned"] / agg["delivered"]) if agg["delivered"] else 0
    print(f"  produced events         : {total}  ({args.tenants} tenants x {args.events_per_tenant} events)")
    print(f"  produce time            : {produce_s:6.2f}s")
    print(f"  drain time              : {drain_s:6.2f}s   ({n} slots = {n} workers)")
    print(f"  consume throughput      : {consumed/drain_s:,.0f} events/s")
    print(f"  read amplification      : {amp:.2f}x   (scanned {agg['scanned']:,} / delivered {agg['delivered']:,}; ideal = N = {n})")
    print("  ---- invariants ----")
    ok = True
    ok &= check("delivered exactly once (no loss, no dup)", consumed == total and distinct_msgs == total,
                f"consumed={consumed}, distinct={distinct_msgs}, produced={total}")
    ok &= check("G1 affinity: each key on exactly one slot", multislot == 0, f"{multislot} keys on >1 slot")
    ok &= check("G1 FIFO: per-key msg_id non-decreasing", outoforder == 0, f"{outoforder} out-of-order")
    return ok


# ---------------------------------------------------------------------------
def run_workers(fn, items):
    threads = [threading.Thread(target=fn, args=(it,)) for it in items]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def check(label, passed, detail=""):
    mark = "PASS" if passed else "FAIL"
    extra = "" if passed else f"  <-- {detail}"
    print(f"    [{mark}] {label}{extra}")
    return passed


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tier", required=True, choices=["a", "b"])
    p.add_argument("--tenants", type=int, default=1000)
    p.add_argument("--workers", type=int, default=8)          # tier A
    p.add_argument("--dups", type=int, default=4)             # tier A
    p.add_argument("--work-ms", type=int, default=3)          # tier A
    p.add_argument("--slots", type=int, default=8)            # tier B (= workers)
    p.add_argument("--events-per-tenant", type=int, default=20)  # tier B
    p.add_argument("--chunk", type=int, default=500)          # events per tick
    p.add_argument("--max-batch", type=int, default=100000)
    args = p.parse_args()

    ok = run_tier_a(args) if args.tier == "a" else run_tier_b(args)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
