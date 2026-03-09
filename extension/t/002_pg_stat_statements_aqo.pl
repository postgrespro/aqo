use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 12;

my $node = PostgreSQL::Test::Cluster->new('test');

$node->init;

$node->append_conf('postgresql.conf', qq{
						aqo.mode = 'disabled'
						aqo.force_collect_stat = 'false'
						log_statement = 'ddl' # reduce size of logs.
						aqo.join_threshold = 0
						pg_stat_statements.track = 'none'
					});
my $query_id;

# Disable connection default settings, forced by PGOPTIONS in AQO Makefile
$ENV{PGOPTIONS}="";

# General purpose variables.
my $res;
my $aqo_res;
my $total_classes;
$node->start();

$node->psql('postgres', "CREATE EXTENSION aqo"); # Error
$node->append_conf('postgresql.conf', qq{
	shared_preload_libraries = 'aqo, pg_stat_statements'
	aqo.mode = 'disabled' # disable AQO on schema creation
});
$node->restart();
$node->safe_psql('postgres', "
	CREATE EXTENSION aqo;
	CREATE EXTENSION pg_stat_statements;
");

# Execute test DDL
$node->psql('postgres', "
	CREATE TABLE aqo_test0(a int, b int, c int, d int);
	WITH RECURSIVE t(a, b, c, d) AS (
		VALUES (0, 0, 0, 0)
		UNION ALL
		SELECT t.a + 1, t.b + 1, t.c + 1, t.d + 1 FROM t WHERE t.a < 2000
	) INSERT INTO aqo_test0 (SELECT * FROM t);
	CREATE INDEX aqo_test0_idx_a ON aqo_test0 (a);
	ANALYZE aqo_test0;
");
$node->psql('postgres', "
	CREATE TABLE trig(
		x double precision,
		sinx double precision,
		cosx double precision);
	WITH RECURSIVE t(a, b, c) AS (
		VALUES (0.0::double precision, 0.0::double precision, 1.0::double precision)
		UNION ALL
		SELECT t.a + pi() / 50, sin(t.a + pi() / 50), cos(t.a + pi() / 50)
		FROM t WHERE t.a < 2 * pi()
	) INSERT INTO trig (SELECT * FROM t);
	CREATE INDEX trig_idx_x ON trig (x);
	ANALYZE trig;
");
$node->psql('postgres', "
	CREATE TABLE department(
		DepartmentID INT PRIMARY KEY NOT NULL,
		DepartmentName VARCHAR(20)
	);
	CREATE TABLE employee (
		LastName VARCHAR(20),
		DepartmentID INT REFERENCES department(DepartmentID)
	);
	INSERT INTO department
	VALUES (31, 'Sales'), (33, 'Engineering'), (34, 'Clerical'),
		(35, 'Marketing');
	INSERT INTO employee
	VALUES ('Rafferty', 31), ('Jones', 33), ('Heisenberg', 33),
		('Robinson', 34), ('Smith', 34), ('Williams', NULL);
");
$node->psql('postgres', "
	ALTER SYSTEM SET aqo.mode = 'learn';
	ALTER SYSTEM SET pg_stat_statements.track = 'all';
	SELECT pg_reload_conf();
");

# Trivial query without any clauses/parameters
$node->safe_psql('postgres', "SELECT * FROM aqo_test0");
$res = $node->safe_psql('postgres', "
	SELECT query FROM pg_stat_statements
	JOIN aqo_queries USING(queryid)
"); # Both extensions have the same QueryID for the query above
is($res, "SELECT * FROM aqo_test0");

# Check number of queries which logged in both extensions.
$aqo_res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_texts
"); # 2 - Common fs and trivial select.
$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM pg_stat_statements
"); # 3 - trivial select and two utility queries above.
is($res - $aqo_res, 1);

$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM pg_stat_statements
	WHERE queryid NOT IN (SELECT queryid FROM aqo_query_texts)
"); # Trivial select and utility query to pg_stat_statements
is($res, 2);

$node->safe_psql('postgres', "
	SELECT * FROM trig WHERE sinx < 0.5 and cosx > -0.5
"); # Log query with two constants
$node->safe_psql('postgres', "
	SELECT count(*) FROM pg_stat_statements
	WHERE query = 'SELECT * FROM trig WHERE sinx < 0.5 and cosx > -0.5'
"); # The pg_stat_statements utility queries are logged too
$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_texts aqt, pg_stat_statements pgss
	WHERE aqt.queryid = pgss.queryid
");
is($res, 4);

$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM pg_stat_statements
	WHERE queryid NOT IN (SELECT queryid FROM aqo_query_texts)
"); # pgss logs queries to AQO tables these AQO are skip
is($res, 4);
$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_queries
	WHERE queryid NOT IN (SELECT queryid FROM pg_stat_statements)
"); # PGSS have logged all queries that AQO logged, expect common fs.
is($res, 1);

# ############################################################################ #
#
# Complex queries with meaningful tables
#
# ############################################################################ #

$node->safe_psql('postgres', "
	SELECT employee.LastName, employee.DepartmentID, department.DepartmentName
	FROM employee
	INNER JOIN department ON employee.DepartmentID = department.DepartmentID;
"); # Log query with a JOIN and a join clause
$node->safe_psql('postgres', "
	EXPLAIN ANALYZE
	SELECT ee.LastName, ee.DepartmentID, dpt.DepartmentName
	FROM employee ee
	INNER JOIN department dpt ON (ee.DepartmentID = dpt.DepartmentID)
	WHERE ee.LastName NOT LIKE 'Wi%';
"); # Use a table aliases, EXPLAIN ANALYZE mode and WHERE clause.
$node->safe_psql('postgres', "
	SELECT ee.LastName, ee.DepartmentID, dpt.DepartmentName
	FROM employee ee
	INNER JOIN department dpt ON (ee.DepartmentID = dpt.DepartmentID)
	WHERE ee.LastName NOT LIKE 'Wi%';
"); # Without EXPLAIN ANALYZE option
$node->safe_psql('postgres', "
	WITH smth AS (
		SELECT a FROM aqo_test0
	) SELECT * FROM employee ee, department dpt, smth
	WHERE (ee.DepartmentID = dpt.DepartmentID)
		AND (ee.LastName NOT LIKE 'Wi%')
		AND (ee.DepartmentID < smth.a);
"); # Use CTE
$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_texts aqt, pg_stat_statements pgss
	WHERE aqt.queryid = pgss.queryid
"); # Check, both extensions added the query with the same query ID.
is($res, 8);

# Check query texts identity.
# TODO: Maybe AQO should use parameterized query text too?
$res = $node->safe_psql('postgres', "
	SELECT count(*)
	FROM aqo_query_texts aqt, pg_stat_statements pgss
	WHERE aqt.queryid = pgss.queryid AND aqt.query_text != pgss.query
"); # PGSS processes a query and generalizes it. So, some queries is diferent
is($res, 6);
$res = $node->safe_psql('postgres', "
	SELECT count(*)
	FROM aqo_query_texts aqt, pg_stat_statements pgss
	WHERE aqt.queryid = pgss.queryid AND aqt.query_text = pgss.query
"); # Non-parameterized queries (without constants in a body of query) will have the same query text.
is($res, 2);

# Check queries hasn't logged by another extension

$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM pg_stat_statements
	WHERE queryid NOT IN (SELECT queryid FROM aqo_queries)
		AND query NOT LIKE '%aqo_quer%'
"); # PGSS logs all the same except queries with AQO-related objects.
is($res, 1); # allow to find shifts in PGSS logic

# TODO: why queries in EXPLAIN ANALYZE mode have different query ID in AQO
# and PGSS extensions?

$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_queries
	WHERE queryid NOT IN (SELECT queryid FROM pg_stat_statements)
");
is($res, 1);

# only first entry in aqo_query_texts has zero hash
$res = $node->safe_psql('postgres', "
	SELECT count(*) FROM aqo_query_texts
	WHERE queryid = 0
");
is($res, 1);

# TODO: check queries with queries in stored procedures

$node->stop();
