use strict;
use warnings;

use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More tests => 1;

my $node = PostgreSQL::Test::Cluster->new('aqotest');
$node->init;
$node->append_conf('postgresql.conf', qq{
						shared_preload_libraries = 'aqo'
						aqo.join_threshold = 0
						aqo.mode = 'learn'
						aqo.show_details = 'off'
						aqo.learn_statement_timeout = 'on'
					});

# Test constants. Default values.
my $TRANSACTIONS = 100;

# Disable connection default settings, forced by PGOPTIONS in AQO Makefile
# $ENV{PGOPTIONS}="";

# Change pgbench parameters according to the environment variable.
if (defined $ENV{TRANSACTIONS})
{
	$TRANSACTIONS = $ENV{TRANSACTIONS};
}

my $query_string = '
CREATE TABLE IF NOT EXISTS aqo_test1(a int, b int);
WITH RECURSIVE t(a, b)
AS (
   VALUES (1, 2)
   UNION ALL
   SELECT t.a + 1, t.b + 1 FROM t WHERE t.a < 10
) INSERT INTO aqo_test1 (SELECT * FROM t);

SET statement_timeout = 10;

CREATE TABLE tmp1 AS SELECT t1.a AS a, t2.a AS b, t3.a AS c
FROM aqo_test1 AS t1, aqo_test1 AS t2, aqo_test1 AS t3
WHERE t1.a = t2.b AND t2.a = t3.b;
DROP TABLE tmp1;
';

$node->start();

$node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS aqo;');

for (1..$TRANSACTIONS) {
	$node->psql('postgres', $query_string);
}

ok(1, "There are no segfaults");

$node->stop();
