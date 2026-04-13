-- pgque lifecycle functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create or replace function pgque.start()
returns void as $$
declare
    v_ticker_id bigint;
    v_maint_id bigint;
    v_dbname text;
begin
    -- Require pg_cron
    if not exists (select 1 from pg_extension where extname = 'pg_cron') then
        raise exception 'pg_cron extension is not installed. '
            'Install pg_cron first: CREATE EXTENSION pg_cron; '
            'Or run pgque.ticker() and maintenance manually.';
    end if;

    -- Idempotent: stop existing jobs first
    perform pgque.stop();

    v_dbname := current_database();

    -- Ticker: every 2 seconds (pg_cron >= 1.5 for sub-minute scheduling)
    select cron.schedule_in_database(
        'pgque_ticker',
        '2 seconds',
        $sql$select pgque.ticker()$sql$,
        v_dbname
    ) into v_ticker_id;

    -- Maintenance: every 30 seconds
    select cron.schedule_in_database(
        'pgque_maint',
        '30 seconds',
        $sql$select f.func_name from pgque.maint_operations() f$sql$,
        v_dbname
    ) into v_maint_id;

    -- Store job IDs in config
    update pgque.config
    set ticker_job_id = v_ticker_id,
        maint_job_id = v_maint_id;

    raise notice 'pgque started: ticker job=%, maint job=%', v_ticker_id, v_maint_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.stop()
returns void as $$
declare
    v_ticker_id bigint;
    v_maint_id bigint;
    v_has_pgcron bool;
begin
    -- Read current job IDs
    select ticker_job_id, maint_job_id
    into v_ticker_id, v_maint_id
    from pgque.config;

    -- Check if pg_cron is available
    select exists (select 1 from pg_extension where extname = 'pg_cron')
    into v_has_pgcron;

    -- Unschedule ticker if it exists
    if v_ticker_id is not null and v_has_pgcron then
        perform cron.unschedule(v_ticker_id);
    end if;

    -- Unschedule maint if it exists
    if v_maint_id is not null and v_has_pgcron then
        perform cron.unschedule(v_maint_id);
    end if;

    -- Clear job IDs regardless (even if pg_cron is gone)
    update pgque.config
    set ticker_job_id = null,
        maint_job_id = null;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.uninstall()
returns void as $$
begin
    -- Stop pg_cron jobs before dropping the schema.
    -- Handle stop() failures gracefully (pg_cron may not be installed,
    -- or jobs may have been removed externally).
    begin
        perform pgque.stop();
    exception when others then
        raise notice 'pgque.uninstall: stop() failed (%), continuing', sqlerrm;
    end;
    -- Drop everything
    drop schema pgque cascade;
    -- Note: roles are not dropped here (they may be in use by other databases)
    raise notice 'pgque uninstalled. Run DROP ROLE IF EXISTS pgque_reader, pgque_writer, pgque_admin; manually if needed.';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.version()
returns text as $$
begin
    return '1.0.0-dev';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
