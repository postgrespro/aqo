use strict;
use warnings;
use TestLib;
use Test::More tests => 6;
use PostgresNode;

my $node = PostgresNode->new('aqotest');
$node->init;
$node->append_conf('postgresql.conf', qq{
						shared_preload_libraries = 'aqo'
						aqo.mode = 'intelligent'
						aqo.log_ignorance = 'on'
					});

#my $result1;

$node->start();

# Check conflicts of accessing to the ML knowledge base
# intelligent mode
$node->safe_psql('postgres', "CREATE EXTENSION aqo");
$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'intelligent'");
$node->command_ok([ 'pgbench', '-i', '-s', '1' ], 'init pgbench tables');
$node->command_ok([ 'pgbench', '-t', "1000", '-c', "20", '-j', "20" ],
	'pgbench in intelligent mode');

$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'controlled'");
$node->command_ok([ 'pgbench', '-t', "1000", '-c', "20", '-j', "20" ],
	'pgbench in controlled mode');

$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'disabled'");
$node->command_ok([ 'pgbench', '-t', "1000", '-c', "20", '-j', "20" ],
	'pgbench in disabled mode');

$node->safe_psql('postgres', "DROP EXTENSION aqo");
$node->safe_psql('postgres', "CREATE EXTENSION aqo");

$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'learn'");
$node->command_ok([ 'pgbench', '-t', "1000", '-c', "20", '-j', "20" ],
	'pgbench in learn mode');

$node->safe_psql('postgres', "ALTER SYSTEM SET aqo.mode = 'frozen'");
$node->command_ok([ 'pgbench', '-t', "1000", '-c', "20", '-j', "20" ],
	'pgbench in frozen mode');

$node->safe_psql('postgres', "DROP EXTENSION aqo");

$node->stop();
