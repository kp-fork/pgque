-- Add queue_max_retries column to pgque.queue
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- The queue table is defined in PgQ's tables.sql.  After the transformed
-- PgQ schema is installed, we add this pgque-specific column.

do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'pgque' and table_name = 'queue'
        and column_name = 'queue_max_retries'
    ) then
        alter table pgque.queue add column queue_max_retries int4;
    end if;
end $$;
