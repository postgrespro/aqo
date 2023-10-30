use strict;
use warnings;

use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More tests => 2;

my $node = PostgreSQL::Test::Cluster->new('aqotest');
$node->init;
$node->append_conf('postgresql.conf', qq{
						shared_preload_libraries = 'aqo'
						log_statement = 'ddl'
						aqo.join_threshold = 0
						aqo.mode = 'learn'
						aqo.show_details = 'on'
						aqo.show_hash = 'on'
						aqo.min_neighbors_for_predicting = 1
						enable_nestloop = 'off'
						enable_mergejoin = 'off'
						enable_material = 'off'
					});

$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION aqo');

# Create tables with correlated datas in columns

$node->safe_psql('postgres', 'CREATE TABLE a (x1 int, x2 int, x3 int);
INSERT INTO a (x1, x2, x3) SELECT mod(ival,10), mod(ival,10), mod(ival,10) FROM generate_series(1,1000) As ival');

$node->safe_psql('postgres', 'CREATE TABLE b (y1 int, y2 int, y3 int);
INSERT INTO b (y1, y2, y3) SELECT mod(ival + 1,10), mod(ival + 1,10), mod(ival + 1,10) FROM generate_series(1,1000) As ival');

my $result;

my $plan = $node->safe_psql('postgres', 'EXPLAIN (analyze true, verbose true)
SELECT a.x1, b.y1, COUNT(*) FROM a, b WHERE a.x2 = b.y2 GROUP BY a.x1, b.y1;');
my @fss = $plan =~ /fss=(-?\d+)/g;

$result = $node->safe_psql('postgres', 'SELECT count(*) FROM aqo_data;');
is($result, 4);

$result = $node->safe_psql('postgres', 'SELECT fss FROM aqo_data;');

my @storage = split(/\n/, $result);

# compare fss from plan and fss from storage
my $test2 = 1;
if (scalar @fss == scalar @storage) {
	foreach my $numb1 (@fss) {
		my $found = 0;

		# check fss not zero
		if ($numb1 == 0) {
			$test2 = 0;
			last;
		}

		foreach my $numb2 (@storage) {
			if ($numb2 == $numb1) {
				$found = 1;
				last;
			}
		}

		if (!$found) {
			$test2 = 0;
			last;
		}
	}
} else {
	$test2 = 0;
}

is($test2, 1);

$node->stop();