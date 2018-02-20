# Adaptive query optimization

Adaptive query optimization is the extension of standard PostgreSQL cost-based
query optimizer. Its basic principle is to use query execution statistics
for improving cardinality estimation. Experimental evaluation shows that this
improvement sometimes provides an enormously large speed-up for rather
complicated queries.

## Installation

In your db:
CREATE EXTENSION aqo;

and modify your postgresql.conf:
shared_preload_libraries = 'aqo.so'

It is essential that library is preloaded during server startup, because
adaptive query optimization has to be enabled on per-database basis instead
of per-connection.

## Usage

Note that the extension works bad with dynamically generated views. If they
appear in workload, please use "aqo.mode='controlled'".

This extension has intelligent self-tuning mode. If you want to rely completely
on it, just add line "aqo.mode = 'intelligent'" into your postgresql.conf.

Now this mode may work not good for rapidly changing data and query
distributions, so it is better to reset extension manually when that happens.

Also please note that intelligent mode is not supposed to work with queries
with dynamically generated structure. Dynamically generated constants are okay.

For handling workloads with dynamically generated query structures the forced
mode "aqo.mode = 'forced'" is provided. We cannot guarantee performance
improvement with this mode, but you may try it nevertheless.

If you want to completely control how PostgreSQL optimizes queries, use controlled
mode "aqo.mode = 'controlled'" and
contrib/aqo/learn_queries.sh file_with_sql_queries.sql "psql -d YOUR_DATABASE"
where file_with_sql_queries.sql is a textfile with queries on which AQO is
supposed to learn. Please use only SELECT queries file_with_sql_queries.sql.
More sophisticated and convenient tool for AQO administration is in the
development now.
If you want to freeze optimizer's behavior (i. e. disable learning under
workload), use "UPDATE aqo_queries SET auto_tuning=false;".
If you want to disable AQO for all queries, you may use
"UPDATE aqo_queries SET use_aqo=false, learn_aqo=false, auto_tuning=false;".

## Advanced tuning

To control query optimization we introduce for each query its type.
We consider that queries belong to the same type if and only if they differ only
in their constants.
One can see an example of query corresponding to the specified query type
in table aqo_query_texts.
select * from aqo_query_texts;

That is why intelligent mode does not work for dynamically generated query
structures: it tries to learn separately how to optimize different query types,
and for dynamical query structure the query types are different, so it will
consume a lot of memory and will not optimize any query properly.

Forced mode forces AQO to ignore query types and optimize them together. On one
hand it lacks of intelligent tuning, so the performance for some queries may
even decrease, on the other hand it may work for dynamic workload and consumes
less memory than the intelligent mode. That is why you may want to use it.

Each query type has its own optimization settings. You can find them in table
aqo_queries.

Auto_tuning setting identifies whether AQO module tries to tune other settings
from aqo_queries for the query type on its own. If the mode is intelligent,
default value for new queries is true. If the mode is not intelligent, new queries
are not appended to aqo_queries automatically, but you can also set auto_tuning
variable to true manually.

Use_aqo setting shows whether AQO cardinalities prediction be used for next
execution of such query type. Disabling of AQO usage is reasonable for that
cases in which query execution time increases after applying AQO. It happens
sometimes because of cost models incompleteness.

Learn_aqo setting shows whether AQO collects statistics for next execution of
such query type. True value may have computational overheads, but it is
essential when AQO model does not fit the data. It happens at the start of AQO
for the new query type or when the data distribution in database is changed.

Fspace_hash setting is for extra advanced AQO tuning. It may be changed manually
to optimize a number of query types using the same model. It may decrease the
amount of memory for models and even query execution performance, but also it
may cause the bad AQO's behavior, so please use it only if you know exactly
what you do.

## Statistics

For forced and intelligent query modes, and for all tracked queries the
statistics is collected. The statistics is cardinality quality, planning and
execution time. For forced mode the statistics for all untracked query types
is stored in common query type with hash 0.

One can see the collected statistics in table aqo_query_stat.
