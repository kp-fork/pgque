\set ON_ERROR_STOP on

-- Test receive/ack/nack API
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- PgQ requires insert, ticker, and receive to be in separate transactions
-- (snapshot visibility). Each DO block is a separate transaction.

-- Step 1: setup
do $$
begin
  perform pgque.create_queue('test_recv');
  perform pgque.register_consumer('test_recv', 'c1');
end $$;

-- Step 2: insert event (separate transaction)
do $$
begin
  perform pgque.insert_event('test_recv', 'test.type', '{"key":"val"}');
end $$;

-- Step 3: ticker (separate transaction to capture the insert)
do $$
begin
  perform pgque.ticker();
end $$;

-- Step 4: receive and verify
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_recv', 'c1', 10)
  loop
    v_count := v_count + 1;
    assert v_msg.type = 'test.type', 'type should be test.type';
    assert v_msg.payload = '{"key":"val"}', 'payload should match';
    assert v_msg.batch_id is not null, 'batch_id should be set';
  end loop;

  assert v_count = 1, 'should receive exactly 1 message, got ' || v_count;

  -- Ack the batch
  perform pgque.ack(v_msg.batch_id);
end $$;

-- Step 5: verify no more messages after ack
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_recv', 'c1', 10)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'should have no more messages after ack';
end $$;

-- Cleanup
do $$
begin
  perform pgque.unregister_consumer('test_recv', 'c1');
  perform pgque.drop_queue('test_recv');
  raise notice 'PASS: receive + ack';
end $$;
