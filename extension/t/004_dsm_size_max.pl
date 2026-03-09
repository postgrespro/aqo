use strict;
use warnings;

use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More tests => 6;

my $node = PostgreSQL::Test::Cluster->new('aqotest');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'aqo'
aqo.mode = 'learn'
log_statement = 'ddl'
aqo.join_threshold = 0
aqo.dsm_size_max = 4
aqo.fs_max_items = 30000
aqo.querytext_max_size = 1000000
});

# Disable connection default settings, forced by PGOPTIONS in AQO Makefile
$ENV{PGOPTIONS}="";

# General purpose variables.
my $long_string = 'a' x 1000000;

$node->start();
$node->psql('postgres', 'CREATE EXTENSION aqo;');

for my $i (1 .. 3) {
	$node->psql('postgres', "select aqo_query_texts_update(" . $i . ", \'" . $long_string . "\');");
}
$node->stop();

$node->adjust_conf('postgresql.conf', 'aqo.dsm_size_max', '1');
is($node->start(fail_ok => 1),
		0, "node fails to start");

$node->adjust_conf('postgresql.conf', 'aqo.dsm_size_max', '4');
is($node->start(),
		1, "node starts");
$node->psql('postgres', 'select * from aqo_reset();');

$long_string = '1, ' x 10000;
for my $i (1 .. 30) {
	$node->psql('postgres', "select aqo_data_update(" . $i . ", 1, 1, '{{1}}', '{1}', '{1}', '{" . $long_string . " 1}');");
}
$node->stop();

$node->adjust_conf('postgresql.conf', 'aqo.dsm_size_max', '1');
is($node->start(fail_ok => 1),
		0, "node fails to start");

$node->adjust_conf('postgresql.conf', 'aqo.dsm_size_max', '4');
is($node->start(),
		1, "node starts");
$node->psql('postgres', 'select * from aqo_reset();');
$node->stop();

# 3000mb (more than 2*31 bytes) overflows 4-byte signed int
$node->adjust_conf('postgresql.conf', 'aqo.dsm_size_max', '3000');
is($node->start(fail_ok => 1), 1, "Large aqo.dsm_size_max doesn't cause integer overflow");
$node->stop();


my $regex;
$long_string = 'a' x 100000;
$regex = qr/.*WARNING:  \[AQO\] Not enough DSA\. AQO was disabled for this query/;
$node->adjust_conf('postgresql.conf', 'aqo.dsm_size_max', '1');
$node->start();
my ($stdout, $stderr);
for my $i (1 .. 20) {
	$node->psql('postgres', "create table a as select s, md5(random()::text) from generate_Series(1,100) s;");
	$node->psql('postgres',
				"SELECT a.s FROM a CROSS JOIN ( SELECT '" . $long_string . "' as long_string) AS extra_rows;",
				stdout => \$stdout, stderr => \$stderr);
	$node->psql('postgres', "drop table a");
}
like($stderr, $regex, 'warning for exceeding the dsa limit');
$node->stop;
done_testing();
