use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 2;

my $node = get_new_node('profiling');
$node->init;
print "create conf";

$node->append_conf('postgresql.conf', qq{
						aqo.mode = 'disabled'
						aqo.profile_classes = -1
						aqo.profile_enable = 'true'
						aqo.force_collect_stat = 'false'
						log_statement = 'ddl' # reduce size of logs.
						aqo.join_threshold = 0
					});
# Test constants.
my $TRANSACTIONS = 100;
my $CLIENTS = 10;
my $THREADS = 10;
my $query_id;

# General purpose variables.
my $res;
my $total_classes;
$node->start();
 # ERROR: AQO allow to load library only on startup
print "Create extension aqo";
$node->psql('postgres', "CREATE EXTENSION aqo");
$node->psql('postgres', "CREATE EXTENSION pg_stat_statements");
print "create preload libraries";
$node->append_conf('postgresql.conf', qq{shared_preload_libraries = 'aqo, pg_stat_statements'});
$node->restart();
$node->psql('postgres', "CREATE EXTENSION aqo");
$node->psql('postgres', "CREATE EXTENSION pg_stat_statements");
$node->psql('postgres', "
	ALTER SYSTEM SET aqo.profile_enable = 'true';
	SELECT pg_reload_conf();
");

$node->psql('postgres', "CREATE TABLE aqo_test0(a int, b int, c int, d int);
WITH RECURSIVE t(a, b, c, d)
AS (
   VALUES (0, 0, 0, 0)
   UNION ALL
   SELECT t.a + 1, t.b + 1, t.c + 1, t.d + 1 FROM t WHERE t.a < 2000
) INSERT INTO aqo_test0 (SELECT * FROM t);
CREATE INDEX aqo_test0_idx_a ON aqo_test0 (a);
ANALYZE aqo_test0;");
$node->psql('postgres', "
	ALTER SYSTEM SET aqo.mode = 'controlled';
");
$res = $node->safe_psql('postgres', "SELECT * FROM aqo_test0");
$res = $node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_statements where query = 'SELECT * FROM aqo_test0'");
is($res, 1); # The same query add in pg_stat_statements
$res = $node->safe_psql('postgres', "SELECT count(*) from aqo_query_texts where query_text = 'SELECT * FROM aqo_test0'");
is($res, 0); # The same query isn't added into aqo_query_texts
$node->stop();