# Acquiring superuser privileges
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

my $node;

# Initialize node
$node = get_new_node('node');
$node->init;
$node->start;

my $query;
my $is_su;

print($node->safe_psql("postgres", "CREATE USER regress_hacker LOGIN"));
$is_su = $node->safe_psql('postgres', undef,
  extra_params => [ '-U', 'regress_hacker', '-c', 'SHOW is_superuser' ]);
diag("The regress_hacker is superuser: " . $is_su . "\n");

$query = q{
CREATE FUNCTION format(f text, r regclass, t text)
RETURNS text
AS $$
BEGIN
  ALTER ROLE regress_hacker SUPERUSER;
  RETURN '';
END
$$ LANGUAGE plpgsql RETURNS NULL ON NULL INPUT;
};

print($node->safe_psql('postgres', undef,
  extra_params => [ '-U', 'regress_hacker', '-c', $query ]) . "\n");

$node->psql("postgres", "CREATE EXTENSION aqo");

$is_su = $node->safe_psql('postgres', undef,
  extra_params => [ '-U', 'regress_hacker', '-c', 'SHOW is_superuser' ]);

diag("The regress_hacker is superuser: " . $is_su . "\n");
ok($is_su eq 'off');
