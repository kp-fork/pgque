-- pgque-unpgque.sql -- Remove pgque from database
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$ begin
    perform pgque.stop();
exception when others then
    null;
end $$;

drop schema if exists pgque cascade;

drop role if exists pgque_reader;
drop role if exists pgque_writer;
drop role if exists pgque_admin;
