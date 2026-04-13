\set ON_ERROR_STOP on
\echo '=== pgque acceptance tests ==='
\echo ''
\echo 'Running: US-1 Basic produce/consume cycle'
\i tests/acceptance/us1_basic_produce_consume.sql
\echo ''
\echo 'Running: US-2 Multiple consumers (fan-out)'
\i tests/acceptance/us2_fan_out.sql
\echo ''
\echo 'Running: US-3 Retry and DLQ flow'
\i tests/acceptance/us3_retry_dlq.sql
\echo ''
\echo 'Running: US-4 Delayed delivery'
\i tests/acceptance/us4_delayed_delivery.sql
\echo ''
\echo 'Running: US-5 Batch processing under load'
\i tests/acceptance/us5_batch_load.sql
\echo ''
\echo 'Running: US-6 Graceful rotation under consumer lag'
\i tests/acceptance/us6_rotation_lag.sql
\echo ''
\echo 'Running: US-7 Transactional exactly-once'
\i tests/acceptance/us7_exactly_once.sql
\echo ''
\echo 'Running: US-8 Install on managed PG'
\i tests/acceptance/us8_managed_install.sql
\echo ''
\echo 'Running: US-9 Observability and health monitoring'
\i tests/acceptance/us9_observability.sql
\echo ''
\echo 'Running: US-10 Idempotent install'
\i tests/acceptance/us10_idempotent_install.sql
\echo ''
\echo '=== ALL ACCEPTANCE TESTS PASSED ==='
