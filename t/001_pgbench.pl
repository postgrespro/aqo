use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 22;

my $node = get_new_node('aqotest');
$node->init;
$node->append_conf('postgresql.conf', qq{
						shared_preload_libraries = 'aqo'
						aqo.mode = 'intelligent'
						log_statement = 'ddl'
						aqo.join_threshold = 0
					});

# Test constants. Default values.
my $TRANSACTIONS = 1000;
my $CLIENTS = 10;
my $THREADS = 10;

# Change pgbench parameters according to the environment variable.
if (defined $ENV{TRANSACTIONS})
{
	$TRANSACTIONS = $ENV{TRANSACTIONS};
}
if (defined $ENV{CLIENTS})
{
	$CLIENTS = $ENV{CLIENTS};
}
if (defined $ENV{THREADS})
{
	$THREADS = $ENV{THREADS};
}

# General purpose variables.
my $res;
my $fss_count;
my $fs_count;
my $fs_samples_count;
my $stat_count;

$node->start();

# The AQO module loaded, but extension still not created.
$node->command_ok([ 'pgbench', '-i', '-s', '1' ], 'init pgbench tables');
$node->command_ok([ 'pgbench', '-t',
					"$TRANSACTIONS", '-c', "$CLIENTS", '-j', "$THREADS" ],
					'pgbench without enabled AQO');

# Check conflicts of accessing to the ML knowledge base
# intelligent mode
$node->safe_psql('postgres', "CREATE EXTENSION aqo");
$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'intelligent'");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");
$node->command_ok([ 'pgbench', '-t',
					"$TRANSACTIONS", '-c', "$CLIENTS", '-j', "$THREADS" ],
					'pgbench in intelligent mode');

$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'controlled'");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");
$node->command_ok([ 'pgbench', '-t',
					"$TRANSACTIONS", '-c', "$CLIENTS", '-j', "$THREADS" ],
					'pgbench in controlled mode');

# ##############################################################################
#
# pgbench on a database with AQO extension in 'disabled' mode.
#
# ##############################################################################

# Cleanup of AQO kbowledge base. Also test correctness of DROP procedure.
$node->safe_psql('postgres', "DROP EXTENSION aqo");
$node->safe_psql('postgres', "CREATE EXTENSION aqo");

# Check: no problems with concurrency in disabled mode.
$node->safe_psql('postgres', "
	ALTER SYSTEM SET aqo.mode = 'disabled';
	SELECT pg_reload_conf();
	SELECT * FROM aqo_reset(); -- Remove old data
");
$node->command_ok([ 'pgbench', '-t',
					"$TRANSACTIONS", '-c', "$CLIENTS", '-j', "$THREADS" ],
					'pgbench in disabled mode');

# Check: no any data added into AQO-related tables.
# Each of aqo_queries and aqo_query_texts tables contains one predefined record.
$fss_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_data;");
$fs_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_queries;");
$fs_samples_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_query_texts;");
$stat_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_query_stat;");
note("counter: $fss_count, $fs_count, $fs_samples_count, $stat_count");
is( (($fss_count == 0) and ($fs_count == 1) and ($fs_samples_count == 1) and ($stat_count == 0)), 1);

# Check: no problems with stats collection in highly concurrent environment.
$node->safe_psql('postgres', "
	ALTER SYSTEM SET aqo.force_collect_stat = 'on';
	SELECT pg_reload_conf();
");
$node->command_ok([ 'pgbench', '-t',
					"$TRANSACTIONS", '-c', "$CLIENTS", '-j', "$THREADS" ],
					'pgbench in disabled mode - 2');

# Check: no any tuples added into the aqo_data table in this mode.
$fss_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_data;");
is( ($fss_count == 0), 1);

# Check: in forced stat collection state AQO writes into aqo_query_stat,
# aqo_queries and aqo_query_texts to give user a chance to find problematic
# queries.
$fs_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_queries");
$fs_samples_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_query_stat");
$stat_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_query_texts");
# This constants looks like magic numbers. But query set of the pgbench test
# is fixed for a long time.
note("fs: $fs_count, $fs_samples_count, $stat_count");
is( (($fs_count == 6) and ($fs_samples_count == 5) and ($stat_count == 6)), 1);

my $analytics = File::Temp->new();
append_to_file($analytics, q{
	\set border random(1, 1E5)
	SELECT count(aid) FROM pgbench_accounts GROUP BY abalance ORDER BY abalance DESC;
	SELECT count(aid) FROM pgbench_accounts GROUP BY abalance HAVING abalance < :border;

	SELECT count(*) FROM pgbench_branches pgbb,
	(SELECT count(aid) AS x FROM pgbench_accounts GROUP BY abalance HAVING abalance < :border) AS q1
	WHERE pgbb.bid = q1.x;
});

# Avoid problems with an error fluctuations during the test above.
$node->safe_psql('postgres', "SELECT aqo_reset()");

# Look for top of problematic queries.
$node->command_ok([ 'pgbench', '-t', "10", '-c', "$CLIENTS", '-j', "$THREADS",
					'-f', "$analytics" ],
					'analytical queries in pgbench (disabled mode)');

$res = $node->safe_psql('postgres',
						"SELECT count(*) FROM aqo_cardinality_error(false) v
						JOIN aqo_query_texts t ON (t.queryid = v.id)
						WHERE v.error > 0. AND t.query_text LIKE '%pgbench_accounts%'");
is($res, 3);
$res = $node->safe_psql('postgres',
						"SELECT * FROM aqo_cardinality_error(false) v
						JOIN aqo_query_texts t ON (t.queryid = v.id)
						WHERE v.error > 0. AND t.query_text LIKE '%pgbench_accounts%'");
note("\n TopN: \n $res \n");
$res = $node->safe_psql('postgres',
						"SELECT v.error, t.query_text FROM aqo_cardinality_error(false) v
						JOIN aqo_query_texts t ON (t.queryid = v.id)
						WHERE v.error > 0.");
note("\n Queries: \n $res \n");
$res = $node->safe_psql('postgres', "SELECT * FROM  public.aqo_execution_time(false) v");
note("\n TIMES: \n $res \n");

$res = $node->safe_psql('postgres',
						"SELECT count(*) FROM public.aqo_execution_time(false) v
						WHERE v.exec_time > 0.");
is($res, 3);

# ##############################################################################
#
# pgbench on a database with AQO in 'learn' mode.
#
# ##############################################################################

$node->safe_psql('postgres', "SELECT aqo_reset()");
$node->safe_psql('postgres', "DROP EXTENSION aqo");
$node->safe_psql('postgres', "CREATE EXTENSION aqo");

$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'learn'");
$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.force_collect_stat = 'off'");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");
$node->command_ok([ 'pgbench', '-t',
					"$TRANSACTIONS", '-c', "$CLIENTS", '-j', "$THREADS" ],
					'pgbench in learn mode');


$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'frozen'");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");
$node->command_ok([ 'pgbench', '-t',
					"$TRANSACTIONS", '-c', "$CLIENTS", '-j', "$THREADS" ],
					'pgbench in frozen mode');

# ##############################################################################
#
# Check procedure of ML-knowledge data cleaning.
#
# ##############################################################################

# Store OIDs of pgbench tables
my $aoid = $node->safe_psql('postgres',
							"SELECT ('pgbench_accounts'::regclass)::oid");
my $boid = $node->safe_psql('postgres',
							"SELECT ('pgbench_branches'::regclass)::oid");
my $toid = $node->safe_psql('postgres',
							"SELECT ('pgbench_tellers'::regclass)::oid");
my $hoid = $node->safe_psql('postgres',
							"SELECT ('pgbench_history'::regclass)::oid");
note("oids: $aoid, $boid, $toid, $hoid");

# Add data into AQO to control that cleaning procedure won't delete nothing extra
$node->safe_psql('postgres', "
	CREATE TABLE detector(a int);
	INSERT INTO detector (a) VALUES (1);
	UPDATE detector SET a = a + 1;
	DELETE FROM detector;
	SELECT count(*) FROM detector;
");

# New queries won't add rows into AQO knowledge base.
$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'disabled'");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");
$node->restart(); # AQO data storage should survive after a restart
$res = $node->safe_psql('postgres', "SHOW aqo.mode");
is($res, 'disabled');

# Number of rows in aqo_data: related to pgbench test and total value.
my $pgb_fss_count = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_data
	WHERE	$aoid = ANY(oids) OR
			$boid = ANY(oids) OR
			$toid = ANY(oids) OR
			$hoid = ANY(oids)
");
$fss_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_data;");

# Number of rows in aqo_queries: related to pgbench test and total value.
my $pgb_fs_count = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_queries
	WHERE fs IN (
		SELECT fs FROM aqo_data
		WHERE
			$aoid = ANY(oids) OR
			$boid = ANY(oids) OR
			$toid = ANY(oids) OR
			$hoid = ANY(oids)
	)
");
$fs_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_queries;");

# Number of rows in aqo_query_texts: related to pgbench test and total value.
my $pgb_fs_samples_count = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_texts
	WHERE queryid IN (
		SELECT fs FROM aqo_data
		WHERE
			$aoid = ANY(oids) OR
			$boid = ANY(oids) OR
			$toid = ANY(oids) OR
			$hoid = ANY(oids)
	)
");
$fs_samples_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_query_texts;");
is($pgb_fs_samples_count > 0, 1, "AQO query texts exists");

# Number of rows in aqo_query_stat: related to pgbench test and total value.
my $pgb_stat_count = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_stat
	WHERE queryid IN (
		SELECT fs FROM aqo_data
		WHERE
			$aoid = ANY(oids) OR
			$boid = ANY(oids) OR
			$toid = ANY(oids) OR
			$hoid = ANY(oids)
	)
");
$stat_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_query_stat;");

$node->safe_psql('postgres', "
	DROP TABLE	pgbench_accounts, pgbench_branches, pgbench_tellers,
				pgbench_history CASCADE;");

# Remove unnecessary AQO knowledge
$node->safe_psql('postgres', "SELECT * FROM aqo_cleanup()");

# Calculate total number of rows in AQO-related tables.
my $new_fs_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_queries;");
my $new_fss_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_data;");
my $new_fs_samples_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_query_texts;");
my $new_stat_count = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_query_stat;");
note("Total AQO rows after dropping pgbench-related tables:
	aqo_queries: ($new_fs_count, $fs_count, $pgb_fs_count),
	aqo_data: ($new_fss_count, $fss_count, $pgb_fss_count),
	aqo_query_texts: ($new_fs_samples_count, $fs_samples_count, $pgb_fs_samples_count),
	aqo_query_stat: ($new_stat_count, $stat_count, $pgb_stat_count)");

# Check total number of rows in AQO knowledge base after removing of
# pgbench-related data.
is($new_fs_count == $fs_count - $pgb_fs_count, 1,
	'Total number of feature spaces');
is($new_fss_count == $fss_count - $pgb_fss_count, 1,
	'Total number of feature subspaces');
is($new_fs_samples_count == $fs_samples_count - $pgb_fs_samples_count, 1,
	'Total number of samples in aqo_query_texts');
is($new_stat_count == $stat_count - $pgb_stat_count, 1,
	'Total number of samples in aqo_query_stat');

$node->safe_psql('postgres', "DROP EXTENSION aqo");

# ##############################################################################
#
# Check CREATE/DROP AQO extension commands in a highly concurrent environment.
#
# ##############################################################################

$node->command_ok([ 'pgbench', '-i', '-s', '1' ], 'init pgbench tables');
my $bank = File::Temp->new();
append_to_file($bank, q{
	\set aid random(1, 100000 * :scale)
	\set bid random(1, 1 * :scale)
	\set tid random(1, 10 * :scale)
	\set delta random(-5000, 5000)
	\set drop_aqo random(0, 5)
	\if :client_id = 0 AND :drop_aqo = 0
		DROP EXTENSION aqo;
	\sleep 10 ms
		CREATE EXTENSION aqo;
	\else
	BEGIN;
	UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
	SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
	UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
	UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
	INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
		VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
	END;
	\endif
});

$node->safe_psql('postgres', "
	CREATE EXTENSION aqo;
	ALTER SYSTEM SET aqo.mode = 'intelligent';
	ALTER SYSTEM SET log_statement = 'ddl';
	SELECT pg_reload_conf();
");
$node->restart();

$node->command_ok([ 'pgbench', '-T',
					"50", '-c', "$CLIENTS", '-j', "$THREADS" , '-f', "$bank"],
					'Conflicts with an AQO dropping command.');

$node->stop();
