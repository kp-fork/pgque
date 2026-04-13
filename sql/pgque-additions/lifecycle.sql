-- pgque lifecycle functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create or replace function pgque.start()
returns void as $$
begin
    raise notice 'pgque.start(): pg_cron integration will be implemented in Sprint 2';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.stop()
returns void as $$
begin
    raise notice 'pgque.stop(): pg_cron integration will be implemented in Sprint 2';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.uninstall()
returns void as $$
begin
    -- Stop pg_cron jobs before dropping the schema.
    -- Sprint 2: stop() must handle its own errors when pg_cron integration lands.
    perform pgque.stop();
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
