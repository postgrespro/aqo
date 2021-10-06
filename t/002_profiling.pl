#
# Tests for the profiling feature
#

use strict;
use warnings;
use TestLib;
use Test::More tests => 20;
use PostgresNode;

my $node = PostgresNode->new('profiling');
$node->init;
$node->append_conf('postgresql.conf', qq{
						shared_preload_libraries = 'aqo'
						aqo.mode = 'disabled'
						aqo.profile_classes = -1
						aqo.profile_enable = 'true'
						aqo.force_collect_stat = 'false'
						aqo.log_ignorance = 'off'
						log_statement = 'ddl' # reduce size of logs.
					});

# Test constants.
my $TRANSACTIONS = 100;
my $CLIENTS = 10;
my $THREADS = 10;

# General purpose variables.
my $res;
my $total_classes;

$node->start();

$node->safe_psql('postgres', "CREATE EXTENSION aqo");

# ##############################################################################
#
# Check enabling profiling without an allocated storage.
#
# ##############################################################################

# True value in config should be changed during startup.
$res = $node->safe_psql('postgres', "SHOW aqo.profile_enable");
is( $res eq 'off', 1);

$node->psql('postgres', "
	ALTER SYSTEM SET aqo.profile_enable = 'true';
	SELECT pg_reload_conf();
");

# Must warn and do nothing. Couldn't possible to enable feature without allocated a buffer.
$res = $node->safe_psql('postgres', "SHOW aqo.profile_enable");
is( $res eq 'off', 1);

# Check aqo_show_classes works correctly.
$total_classes = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_show_classes();");
is($total_classes, 0);

# No execution problems, no classes removed.
$total_classes = $node->safe_psql('postgres', "SELECT * FROM aqo_clear_classes()");
is($total_classes, -1);

# ##############################################################################
#
# Check profiling.
#
# ##############################################################################

$node->safe_psql('postgres', "
	ALTER SYSTEM SET aqo.profile_classes = 100;
	ALTER SYSTEM SET aqo.profile_enable = 'off';
");
$node->restart();
$res = $node->safe_psql('postgres', "SHOW aqo.profile_classes");
is( $res eq '100', 1);
$res = $node->safe_psql('postgres', "SHOW aqo.profile_enable");
is( $res eq 'off', 1);
$node->psql('postgres', "
	ALTER SYSTEM SET aqo.profile_enable = 'true';
	ALTER SYSTEM SET aqo.force_collect_stat = 'true'; -- want to see query texts.
	SELECT pg_reload_conf();
");
$res = $node->safe_psql('postgres', "SHOW aqo.profile_enable");
is( $res eq 'on', 1);
$res = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_show_classes()");
is($res, 0); # The same query isn't fall into statistics.
$res = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_show_classes()");
is($res, 1); # On next execution we can see it.
$res = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_show_classes()");
is($res, 1); # The same query class can't increase profiling entries.
$res = $node->safe_psql('postgres', "SELECT query_text FROM aqo_show_classes() q1, aqo_query_texts q2 WHERE q1.query_hash = q2.query_hash");
is($res, 'SELECT count(*) FROM aqo_show_classes()'); # check query text.

# DROP EXTENSION must clean profiling buffer.
$node->safe_psql('postgres', "
	DROP EXTENSION aqo;
	CREATE EXTENSION aqo;
");
$res = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_show_classes()");
is($res, 0);

# TODO: exclude queries that involves AQO-related functions from profiling.
$res = $node->safe_psql('postgres', "SELECT * FROM aqo_clear_classes()");
$res = $node->safe_psql('postgres', "SELECT count(*) FROM aqo_show_classes()");
is($res, 1); # The same query isn't fall into statistics.

$node->command_ok([ 'pgbench', '-i', '-s', '1' ], 'init pgbench tables');
$res = $node->safe_psql('postgres', "SELECT * FROM aqo_clear_classes()");

$node->command_ok([ 'pgbench', '-t', "$TRANSACTIONS" ],
'check a number of registered transactions during a single-threaded pgbench test');
$res = $node->safe_psql('postgres', "SELECT sum(counter) FROM aqo_show_classes()");
is($res, 502);

$res = $node->safe_psql('postgres', "SELECT * FROM aqo_clear_classes()");
is($res, 8);

$node->command_ok([ 'pgbench', '-t', "$TRANSACTIONS",
					'-c', "$CLIENTS", '-j', "$THREADS" ],
'check a number of registered transactions during a multithreaded pgbench test');
$res = $node->safe_psql('postgres', "SELECT sum(counter) FROM aqo_show_classes()");
is($res, 5002);

$res = $node->safe_psql('postgres', "SELECT * FROM aqo_clear_classes()");
is($res, 8); # Should get the same number of classes as in single-threaded test.

$node->stop();
