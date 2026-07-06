-- pgque-api/send_idem.sql -- Producer idempotency (TTL dedup window)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Implements Phase 1B producer-side dedup (blueprints/idempotency/SPEC.md):
--   pgque.idem                                 -- internal claim table
--   pgque.send_idem(queue, type, payload, idem_key, ttl, partition_key)
--                                              -- text + jsonb overloads
--   pgque.maint_idem()                         -- GC, whole table
--   pgque.maint_idem(queue_name)               -- GC, one queue (maint hook)
--
-- A send carrying an idempotency key already used for the same queue within
-- the TTL window appends nothing and returns the ORIGINAL event id with
-- deduped = true. Concurrent producers racing on one key resolve to exactly
-- one insert: the primary key on (queue_id, idem_key) plus a single atomic
-- upsert serialize them -- no advisory lock, no subtransaction (Key Design
-- Rule 4). The claim and the event append share one transaction, so a
-- failed append rolls the claim back too: a crash can never leave a claimed
-- key with no event (which would silently suppress a never-delivered job).
--
-- KEY-SCOPE HAZARD (US-13.2 -- read before choosing keys):
-- Dedup is an EXACT match on (queue, idem_key). The key MUST encode the
-- desired EFFECT, not just the entity. Key on the entity alone
-- ('migrate:tenant1'), ship migration v1, then ship v2 inside the TTL
-- window -- the v2 send collides with v1's live key and is SILENTLY
-- suppressed: the tenant never gets v2 until the window expires. Correct
-- key includes every dimension that changes the intended work, e.g.
-- 'migrate:<tenant>:<target_schema_version>'. The TTL is for collapsing a
-- burst of the SAME effect, not for rate-limiting distinct effects.
--
-- Composition with partition keys (US-12): i_partition_key rides ev_extra1
-- (same slot routing as pgque.send(queue, type, payload, partition_key));
-- the idem key rides ev_extra2 for observability. Neither feature requires
-- the other.
--
-- Consumer mutual exclusion (US-13.5) is the complementary layer: dedup
-- keeps the log small, a per-key pg_try_advisory_xact_lock + an idempotent
-- handler keep the work correct if a duplicate slips in past the window.

-- Internal claim table: one row per live (queue, idem_key) window.
-- App roles never touch it -- all access goes through the SECURITY DEFINER
-- functions below. Expected live size ~= produce_rate * ttl (tiny for the
-- migration-storm case: tens of thousands of rows for a 1 hour window).
create table if not exists pgque.idem (
    queue_id   int4        not null references pgque.queue (queue_id)
                           on delete cascade,
    idem_key   text        not null,
    event_id   int8,
    expires_at timestamptz not null,
    primary key (queue_id, idem_key)
);

-- GC scans by expiry; serves both the whole-table and per-queue sweeps.
create index if not exists idem_expires_at_idx on pgque.idem (expires_at);

/* Lock the claim table down: pgque_reader holds a blanket select on
   pre-existing pgque tables (roles.sql), so revoke explicitly no matter
   where this file lands in the install order. Revokes are idempotent. */
revoke all on table pgque.idem from public;
revoke all on table pgque.idem from pgque_reader, pgque_writer, pgque_admin;

-- pgque.send_idem(queue, type, payload text, idem_key, ttl, partition_key)
-- Fast path, opaque textual payload (same conventions as pgque.send(text)).
--
-- Returns exactly one row:
--   deduped = false, event_id = <new id>      -- first use in the window
--   deduped = true,  event_id = <original id> -- duplicate; nothing appended
--
-- The claim is one atomic upsert: a fresh key inserts, an EXPIRED key is
-- reclaimed via the conditional do-update, a LIVE key updates nothing and
-- returns no row (dedup). Fresh insert vs reclaim need no distinction in
-- the result -- both mean "this send owns the window" -- so no xmax trick
-- is needed. On the dedup path the original event id is read back in a
-- follow-up statement: a loser blocked on the winner's in-flight claim
-- resumes only after the winner's whole transaction commits, so the row it
-- then reads already carries the winner's event_id.
create or replace function pgque.send_idem(
    i_queue text, i_type text, i_payload text, i_idem_key text,
    i_ttl interval default '1 hour', i_partition_key text default null)
returns table (event_id int8, deduped boolean) as $$
declare
    v_queue_id int4;
    v_extra_maint text[];
    v_claimed boolean;
    v_event_id int8;
begin
    if i_idem_key is null then
        raise exception 'idem_key must not be null';
    end if;
    if i_ttl is null or i_ttl <= interval '0' then
        raise exception 'ttl must be a positive interval';
    end if;

    select q.queue_id, q.queue_extra_maint
    into v_queue_id, v_extra_maint
    from pgque.queue q
    where q.queue_name = i_queue;
    if not found then
        raise exception 'queue not found: %', i_queue;
    end if;

    /* Self-wire GC (US-13.4): register pgque.maint_idem in this queue's
       queue_extra_maint so the stock pgque.maint() runner reaps expired
       claims -- no edit to maint.sql. Steady state costs nothing: the
       check runs on the row already fetched above, and the one-time update
       re-checks under the row lock so concurrent first-sends stay safe. */
    if not (coalesce(v_extra_maint, '{}'::text[]) @> array['pgque.maint_idem']) then
        update pgque.queue q
        set queue_extra_maint =
            coalesce(q.queue_extra_maint, '{}'::text[])
            || array['pgque.maint_idem']
        where q.queue_id = v_queue_id
          and not (coalesce(q.queue_extra_maint, '{}'::text[])
                   @> array['pgque.maint_idem']);
    end if;

    /* Atomic claim (I2): the unique key serializes concurrent producers;
       the conditional do-update lets only an EXPIRED key be reclaimed. */
    insert into pgque.idem as k (queue_id, idem_key, event_id, expires_at)
    values (v_queue_id, i_idem_key, null, now() + i_ttl)
    on conflict (queue_id, idem_key) do update
        set event_id = excluded.event_id,
            expires_at = excluded.expires_at
        where k.expires_at <= now()
    returning true into v_claimed;

    if v_claimed then
        /* Append (I3): same transaction as the claim -- if this insert
           fails, the claim rolls back with it and the key stays usable. */
        v_event_id := pgque.insert_event(
            i_queue, i_type, i_payload, i_partition_key, i_idem_key,
            null, null);

        /* Store the id for later dedup responses. This row was written by
           this transaction two statements ago -- the lock is already held,
           so this is contention-free (and invisible to others until the
           claim+append pair commits as one). */
        update pgque.idem k
        set event_id = v_event_id
        where k.queue_id = v_queue_id
          and k.idem_key = i_idem_key;

        event_id := v_event_id;
        deduped := false;
    else
        /* Dedup: nothing appended; report the original event id. The row
           can only be missing in the pathological corner where the claim
           expired and was GC'd between these two statements (sub-second
           TTLs); event_id is then null but deduped stays true. */
        select k.event_id into v_event_id
        from pgque.idem k
        where k.queue_id = v_queue_id
          and k.idem_key = i_idem_key;

        event_id := v_event_id;
        deduped := true;
    end if;
    return next;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function
    pgque.send_idem(text, text, text, text, interval, text) from public;

-- pgque.send_idem(queue, type, payload jsonb, idem_key, ttl, partition_key)
-- JSON payload variant, opt-in via explicit ::jsonb cast (untyped literals
-- resolve to the text overload -- see the note in send.sql).
create or replace function pgque.send_idem(
    i_queue text, i_type text, i_payload jsonb, i_idem_key text,
    i_ttl interval default '1 hour', i_partition_key text default null)
returns table (event_id int8, deduped boolean) as $$
begin
    return query
    select s.event_id, s.deduped
    from pgque.send_idem(
        i_queue, i_type, i_payload::text, i_idem_key, i_ttl,
        i_partition_key) s;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function
    pgque.send_idem(text, text, jsonb, text, interval, text) from public;

-- pgque.maint_idem(queue_name) -- reap expired claims for one queue.
-- Shaped for the pgque.maint_operations() extra-maint hook: takes the queue
-- name, deletes a bounded batch, returns 1 to request another immediate
-- pass when the batch came back full, else 0. send_idem() registers it in
-- queue_extra_maint automatically; the ownership check in pgque.maint()
-- passes because this function and maint() share the install owner.
create or replace function pgque.maint_idem(i_queue_name text)
returns integer as $$
declare
    v_queue_id int4;
    v_deleted integer;
begin
    select queue_id into v_queue_id
    from pgque.queue
    where queue_name = i_queue_name;
    if not found then
        return 0;
    end if;

    delete from pgque.idem k
    where (k.queue_id, k.idem_key) in (
        select d.queue_id, d.idem_key
        from pgque.idem d
        where d.queue_id = v_queue_id
          and d.expires_at < now()
        limit 10000);
    get diagnostics v_deleted = row_count;

    return case when v_deleted >= 10000 then 1 else 0 end;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.maint_idem(text) from public;

-- pgque.maint_idem() -- reap expired claims across all queues.
-- Standalone entry point for operators scheduling GC directly (pg_cron or
-- any external scheduler) instead of relying on the per-queue hook above.
-- Same bounded-batch contract: returns 1 when a full batch was deleted
-- (call again), 0 when the backlog is drained.
create or replace function pgque.maint_idem()
returns integer as $$
declare
    v_deleted integer;
begin
    delete from pgque.idem k
    where (k.queue_id, k.idem_key) in (
        select d.queue_id, d.idem_key
        from pgque.idem d
        where d.expires_at < now()
        limit 10000);
    get diagnostics v_deleted = row_count;

    return case when v_deleted >= 10000 then 1 else 0 end;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.maint_idem() from public;

-- Grants: send_idem is a producer surface -> pgque_writer (same rationale
-- as send()); maint_idem mirrors maint() -> pgque_admin.
grant execute on function
    pgque.send_idem(text, text, text, text, interval, text)  to pgque_writer;
grant execute on function
    pgque.send_idem(text, text, jsonb, text, interval, text) to pgque_writer;
grant execute on function pgque.maint_idem()     to pgque_admin;
grant execute on function pgque.maint_idem(text) to pgque_admin;
