-- Tests for prepared transaction recovery
SET citus.next_shard_id TO 1220000;

-- Disable auto-recovery for the initial tests
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

-- Ensure pg_dist_transaction is empty
SELECT recover_prepared_transactions();

-- Create some "fake" prepared transactions to recover
\c - - - :worker_1_port

BEGIN;
CREATE TABLE should_abort (value int);
PREPARE TRANSACTION 'citus_0_should_abort';

BEGIN;
CREATE TABLE should_commit (value int);
PREPARE TRANSACTION 'citus_0_should_commit';

BEGIN;
CREATE TABLE should_be_sorted_into_middle (value int);
PREPARE TRANSACTION 'citus_0_should_be_sorted_into_middle';

\c - - - :master_port
-- Add "fake" pg_dist_transaction records and run recovery
INSERT INTO pg_dist_transaction VALUES (1, 'citus_0_should_commit');
INSERT INTO pg_dist_transaction VALUES (1, 'citus_0_should_be_forgotten');

SELECT recover_prepared_transactions();
SELECT count(*) FROM pg_dist_transaction;

-- Confirm that transactions were correctly rolled forward
\c - - - :worker_1_port
SELECT count(*) FROM pg_tables WHERE tablename = 'should_abort';
SELECT count(*) FROM pg_tables WHERE tablename = 'should_commit';

\c - - - :master_port
SET citus.shard_replication_factor TO 2;
SET citus.shard_count TO 2;
SET citus.multi_shard_commit_protocol TO '2pc';

-- create_distributed_table should add 2 recovery records (1 connection per node)
CREATE TABLE test_recovery (x text);
SELECT create_distributed_table('test_recovery', 'x');
SELECT count(*) FROM pg_dist_transaction;

-- create_reference_table should add another 2 recovery records
CREATE TABLE test_recovery_ref (x text);
SELECT create_reference_table('test_recovery_ref');
SELECT count(*) FROM pg_dist_transaction;

SELECT recover_prepared_transactions();

-- plain INSERT does not use 2PC
INSERT INTO test_recovery VALUES ('hello');
SELECT count(*) FROM pg_dist_transaction;

-- Aborted DDL commands should not write transaction recovery records
BEGIN;
ALTER TABLE test_recovery ADD COLUMN y text;
ROLLBACK;
SELECT count(*) FROM pg_dist_transaction;

-- Committed DDL commands should write 4 transaction recovery records
ALTER TABLE test_recovery ADD COLUMN y text;

SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();
SELECT count(*) FROM pg_dist_transaction;

-- Committed master_modify_multiple_shards should write 4 transaction recovery records
BEGIN;
SELECT master_modify_multiple_shards($$UPDATE test_recovery SET y = 'world'$$); 
ROLLBACK;
SELECT count(*) FROM pg_dist_transaction;

SELECT master_modify_multiple_shards($$UPDATE test_recovery SET y = 'world'$$);

SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();
SELECT count(*) FROM pg_dist_transaction;

-- Aborted INSERT..SELECT should not write transaction recovery records
BEGIN;
INSERT INTO test_recovery SELECT x, 'earth' FROM test_recovery;
ROLLBACK;
SELECT count(*) FROM pg_dist_transaction;

-- Committed INSERT..SELECT should write 4 transaction recovery records
INSERT INTO test_recovery SELECT x, 'earth' FROM test_recovery;

SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- Committed INSERT..SELECT via coordinator should write 4 transaction recovery records
INSERT INTO test_recovery (x) SELECT 'hello-'||s FROM generate_series(1,100) s;

SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- Committed COPY should write 4 transaction records
COPY test_recovery (x) FROM STDIN CSV;
hello-0
hello-1
\.

SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- Create a single-replica table to enable 2PC in multi-statement transactions
CREATE TABLE test_recovery_single (LIKE test_recovery);
SELECT create_distributed_table('test_recovery_single', 'x');

-- Multi-statement transactions should write 2 transaction recovery records
BEGIN;
INSERT INTO test_recovery_single VALUES ('hello-0');
INSERT INTO test_recovery_single VALUES ('hello-2');
COMMIT;
SELECT count(*) FROM pg_dist_transaction;

-- Test whether auto-recovery runs
ALTER SYSTEM SET citus.recover_2pc_interval TO 10;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
SELECT count(*) FROM pg_dist_transaction;

ALTER SYSTEM RESET citus.recover_2pc_interval;
SELECT pg_reload_conf();

DROP TABLE test_recovery_ref;
DROP TABLE test_recovery;
DROP TABLE test_recovery_single;
