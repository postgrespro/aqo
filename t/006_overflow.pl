use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 4;

my $node = PostgreSQL::Test::Cluster->new('aqotest');
$node->init;
$node->append_conf('postgresql.conf', qq{
	shared_preload_libraries = 'aqo'
	aqo.join_threshold = 0
	aqo.mode = 'frozen'
	aqo.show_details = 'on'
	aqo.dsm_size_max = 10
	aqo.force_collect_stat = 'on'
	aqo.fs_max_items = 3
	aqo.fss_max_items = 10
});

# General purpose variables.
my $res;
my $mode;

# Disable default settings, forced by PGOPTIONS in AQO Makefile
$ENV{PGOPTIONS}="";

$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION aqo');

$mode = $node->safe_psql('postgres',"show aqo.mode");
like($mode, qr/frozen/);

$node->safe_psql('postgres', 'CREATE TABLE a (x int);
INSERT INTO a (x) SELECT mod(ival,10) FROM generate_series(1,1000) As ival');

$res = $node->safe_psql('postgres', 'EXPLAIN ANALYZE SELECT x FROM a WHERE x < 5;');
like($res, qr/AQO mode: FROZEN/);

$res = $node->safe_psql('postgres', 'EXPLAIN ANALYZE SELECT count(x) FROM a WHERE x > 5;');
like($res, qr/AQO mode: FROZEN/);

$mode = $node->safe_psql('postgres',"show aqo.mode");
like($mode, qr/frozen/);

$node->stop();
done_testing();
