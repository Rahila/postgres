# Copyright (c) 2025, PostgreSQL Global Development Group

# Test suite for testing memory context statistics reporting

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
       plan skip_all => 'Injection points not supported by this build';
}
my $psql_err;
# Create and start a cluster with one node
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(allows_streaming => 1);
# max_connections need to be bumped in order to accommodate for pgbench clients
# and log_statement is dialled down since it otherwise will generate enormous
# amounts of logging. Page verification failures are still logged.
$node->append_conf(
       'postgresql.conf',
       qq[
max_connections = 100
log_statement = none
]);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_memcontext_reporting;');
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
# Attaching to a client process injection point that throws an error
$node->safe_psql('postgres', "select injection_points_attach('memcontext-client-crash', 'error');");

my $pid = $node->safe_psql('postgres', "SELECT pid from pg_stat_activity where backend_type='checkpointer'");
print "PID";
print $pid;

#Client should have thrown error
$node->psql('postgres', qq(select pg_get_process_memory_contexts($pid, true);), stderr => \$psql_err);
like ( $psql_err, qr/error triggered for injection point memcontext-client-crash/);

#Query the same process after detaching the injection point, using some other client and it should succeed.
$node->safe_psql('postgres', "select injection_points_detach('memcontext-client-crash');");
my $topcontext_name = $node->safe_psql('postgres', "select name from pg_get_process_memory_contexts($pid, true) where path = '{1}';");
ok($topcontext_name = 'TopMemoryContext');

# Attaching to a target process injection point that throws an error
$node->safe_psql('postgres', "select injection_points_attach('memcontext-server-crash', 'error');");

#Server should have thrown error
$node->psql('postgres', qq(select pg_get_process_memory_contexts($pid, true);), stderr => \$psql_err);

#Query the same process after detaching the injection point, using some other client and it should succeed.
$node->safe_psql('postgres', "select injection_points_detach('memcontext-server-crash');");
$topcontext_name = $node->safe_psql('postgres', "select name from pg_get_process_memory_contexts($pid, true) where path = '{1}';");
ok($topcontext_name = 'TopMemoryContext');
done_testing();
