use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 4;
print "start";
my $node = PostgreSQL::Test::Cluster->new('profiling');
$node->init;
print "create conf";

$node->append_conf('postgresql.conf', qq{
						aqo.mode = 'disabled'
						aqo.profile_classes = -1
						aqo.profile_enable = 'true'
						aqo.force_collect_stat = 'false'
						log_statement = 'ddl' # reduce size of logs.
					});
# Test constants.
my $TRANSACTIONS = 100;
my $CLIENTS = 10;
my $THREADS = 10;
my $query_id;

# General purpose variables.
my $res;
my $total_classes;

# Check: allow to load the libraries only on startup
$node->start();
$node->psql('postgres', "CREATE EXTENSION aqo");
$node->psql('postgres', "CREATE EXTENSION pg_stat_statements");

$node->append_conf('postgresql.conf', qq{
	shared_preload_libraries = 'aqo, pg_stat_statements'
	aqo.mode = 'learn' # unconditional learning
});
$node->restart();
$node->psql('postgres', "CREATE EXTENSION aqo");
$node->psql('postgres', "CREATE EXTENSION pg_stat_statements");

$node->psql('postgres', "CREATE TABLE aqo_test0(a int, b int, c int, d int);
WITH RECURSIVE t(a, b, c, d)
AS (
   VALUES (0, 0, 0, 0)
   UNION ALL
   SELECT t.a + 1, t.b + 1, t.c + 1, t.d + 1 FROM t WHERE t.a < 2000
) INSERT INTO aqo_test0 (SELECT * FROM t);
CREATE INDEX aqo_test0_idx_a ON aqo_test0 (a);
ANALYZE aqo_test0;");

$res = $node->safe_psql('postgres', "SELECT * FROM aqo_test0");
$res = $node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_statements where query = 'SELECT * FROM aqo_test0'");

# Check number of queries which logged in both extensions.
$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_texts aqt, pg_stat_statements pgss
	WHERE aqt.query_hash = pgss.queryid
");
is($res, 3);

# TODO: Maybe AQO should parameterize query text too?
$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_texts aqt, pg_stat_statements pgss
	WHERE aqt.query_hash = pgss.queryid AND aqt.query_text = pgss.query
");
is($res, 1);

# Just fix a number of differences
$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_texts
	WHERE query_hash NOT IN (SELECT queryid FROM pg_stat_statements)
");
is($res, 1);

$res = $node->safe_psql('postgres', "
	SELECT query_text FROM aqo_query_texts
	WHERE query_hash NOT IN (SELECT queryid FROM pg_stat_statements)
");
note($res); # Just see differences

$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM pg_stat_statements
	WHERE queryid NOT IN (SELECT query_hash FROM aqo_query_texts)
");
is($res, 8);

$res = $node->safe_psql('postgres', "
	SELECT query FROM pg_stat_statements
	WHERE queryid NOT IN (SELECT query_hash FROM aqo_query_texts)
");
note($res); # Just see differences

$node->stop();
