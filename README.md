# Adaptive query optimization

Adaptive query optimization is the extension of standard PostgreSQL cost-based
query optimizer. Its basic principle is to use query execution statistics
for improving cardinality estimation. Experimental evaluation shows that this
improvement sometimes provides an enormously large speed-up for rather
complicated queries.

## Installation

The module works with PostgreSQL 9.6 and above.
To avoid compatibility issues, the following branches in the git-repository are allocated:
* `stable9_6`.
* `stable11` - for PG v10 and v11.
* `stable12` - for PG v12.
* `stable13` - for PG v13.
* `stable14` - for PG v14.
* `stable15` - for PG v15.
* the `master` branch of the AQO repository correctly works with PGv15 and the PostgreSQL `master` branch.

The module contains a patch and an extension. Patch has to be applied to the
sources of PostgresSQL. Patch affects header files, that is why PostgreSQL
must be rebuilt completely after applying the patch (`make clean` and
`make install`).
Extension has to be unpacked into contrib directory and then to be compiled and
installed with `make install`.

```
cd postgresql-9.6                                                # enter postgresql source directory
git clone https://github.com/postgrespro/aqo.git contrib/aqo        # clone aqo into contrib
patch -p1 --no-backup-if-mismatch < contrib/aqo/aqo_pg<version>.patch  # patch postgresql
make clean && make && make install                               # recompile postgresql
cd contrib/aqo                                                   # enter aqo directory
make && make install                                             # install aqo
make check                                              # check whether it works correctly (optional)
```

Tag `version` at the patch name corresponds to suitable PostgreSQL release.
For PostgreSQL 9.6 use the 'aqo_pg9_6.patch' file; PostgreSQL 10 use aqo_pg10.patch; for PostgreSQL 11 use aqo_pg11.patch and so on.
Also, you can see git tags at the master branch for more accurate definition of
suitable PostgreSQL version.

In your database:

`CREATE EXTENSION aqo;`

Modify your postgresql.conf:

`shared_preload_libraries = 'aqo'`

and restart PostgreSQL.

It is essential that library is preloaded during server startup, because
adaptive query optimization must be enabled on per-cluster basis instead
of per-database.

## Usage

The typical case is follows: you have complicated query, which executes too
long. `EXPLAIN ANALYZE` shows, that the possible reason is bad cardinality
estimation.

Example:
```
                                                                             QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=15028.15..15028.16 rows=1 width=96) (actual time=8168.188..8168.189 rows=1 loops=1)
   ->  Nested Loop  (cost=8.21..15028.14 rows=1 width=48) (actual time=199.500..8167.708 rows=88 loops=1)
         ->  Nested Loop  (cost=7.78..12650.75 rows=5082 width=37) (actual time=0.682..3015.721 rows=785477 loops=1)
               Join Filter: (t.id = ci.movie_id)
               ->  Nested Loop  (cost=7.21..12370.11 rows=148 width=41) (actual time=0.666..404.791 rows=14165 loops=1)
                     ->  Nested Loop  (cost=6.78..12235.17 rows=270 width=20) (actual time=0.645..146.855 rows=35548 loops=1)
                           ->  Seq Scan on keyword k  (cost=0.00..3632.40 rows=8 width=20) (actual time=0.126..29.117 rows=8 loops=1)
                                 Filter: (keyword = ANY ('{superhero,sequel,second-part,marvel-comics,based-on-comic,tv-special,fight,violence}'::text[]))
                                 Rows Removed by Filter: 134162
                           ->  Bitmap Heap Scan on movie_keyword mk  (cost=6.78..1072.32 rows=303 width=8) (actual time=0.919..13.800 rows=4444 loops=8)
                                 Recheck Cond: (keyword_id = k.id)
                                 Heap Blocks: exact=23488
                                 ->  Bitmap Index Scan on keyword_id_movie_keyword  (cost=0.00..6.71 rows=303 width=0) (actual time=0.535..0.535 rows=4444 loops=8)
                                       Index Cond: (keyword_id = k.id)
                     ->  Index Scan using title_pkey on title t  (cost=0.43..0.49 rows=1 width=21) (actual time=0.007..0.007 rows=0 loops=35548)
                           Index Cond: (id = mk.movie_id)
                           Filter: (production_year > 2000)
                           Rows Removed by Filter: 1
               ->  Index Scan using movie_id_cast_info on cast_info ci  (cost=0.56..1.47 rows=34 width=8) (actual time=0.009..0.168 rows=55 loops=14165)
                     Index Cond: (movie_id = mk.movie_id)
         ->  Index Scan using name_pkey on name n  (cost=0.43..0.46 rows=1 width=19) (actual time=0.006..0.006 rows=0 loops=785477)
               Index Cond: (id = ci.person_id)
               Filter: (name ~~ '%Downey%Robert%'::text)
               Rows Removed by Filter: 1
 Planning time: 40.047 ms
 Execution time: 8168.373 ms
(26 rows)
```

Then you can use the following pattern:
```
BEGIN;
SET aqo.mode = 'learn';
EXPLAIN ANALYZE <query>;
RESET aqo.mode;
-- ... do EXPLAIN ANALYZE <query> while cardinality estimations in the plan are bad
--                                      and the plan is bad
COMMIT;
```
**_Warning:_** execute query until plan stops changing!

When the plan stops changing, you can often observe performance improvement:
```
                                                                              QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=112883.89..112883.90 rows=1 width=96) (actual time=738.731..738.731 rows=1 loops=1)
   ->  Nested Loop  (cost=1.85..112883.23 rows=88 width=48) (actual time=73.826..738.618 rows=88 loops=1)
         ->  Nested Loop  (cost=1.43..110496.69 rows=5202 width=36) (actual time=72.917..723.994 rows=5202 loops=1)
               Join Filter: (t.id = mk.movie_id)
               ->  Nested Loop  (cost=0.99..110046.39 rows=306 width=40) (actual time=72.902..720.310 rows=306 loops=1)
                     ->  Nested Loop  (cost=0.56..109820.42 rows=486 width=19) (actual time=72.856..717.429 rows=486 loops=1)
                           ->  Seq Scan on name n  (cost=0.00..107705.93 rows=2 width=19) (actual time=72.819..717.148 rows=2 loops=1)
                                 Filter: (name ~~ '%Downey%Robert%'::text)
                                 Rows Removed by Filter: 4167489
                           ->  Index Scan using person_id_cast_info on cast_info ci  (cost=0.56..1054.82 rows=243 width=8) (actual time=0.024..0.091 rows=243 loops=2)
                                 Index Cond: (person_id = n.id)
                     ->  Index Scan using title_pkey on title t  (cost=0.43..0.45 rows=1 width=21) (actual time=0.005..0.006 rows=1 loops=486)
                           Index Cond: (id = ci.movie_id)
                           Filter: (production_year > 2000)
                           Rows Removed by Filter: 0
               ->  Index Scan using movie_id_movie_keyword on movie_keyword mk  (cost=0.43..1.26 rows=17 width=8) (actual time=0.004..0.008 rows=17 loops=306)
                     Index Cond: (movie_id = ci.movie_id)
         ->  Index Scan using keyword_pkey on keyword k  (cost=0.42..0.45 rows=1 width=20) (actual time=0.003..0.003 rows=0 loops=5202)
               Index Cond: (id = mk.keyword_id)
               Filter: (keyword = ANY ('{superhero,sequel,second-part,marvel-comics,based-on-comic,tv-special,fight,violence}'::text[]))
               Rows Removed by Filter: 1
 Planning time: 51.333 ms
 Execution time: 738.904 ms
(23 rows)
```

The settings system in AQO works with normalised queries, i. e. queries with
removed constants. For example, the normalised version of
`SELECT * FROM tbl WHERE a < 25 AND b = 'str';`
is
`SELECT * FROM tbl WHERE a < CONST and b = CONST;`

So the queries have equal normalisation if and only if they differ only
in their constants.

Each normalised query has its own hash. The correspondence between normalised
query hash and query text is stored in aqo_query_texts table:
```
SELECT * FROM aqo_query_texts;
```
```
 query_hash  |                                query_text
-------------+----------------------------------------------------------------------------
           0 | COMMON feature space (do not delete!)
 -1104999304 | SELECT                                                                    +
             |     MIN(k.keyword) AS movie_keyword,                                      +
             |     MIN(n.name) AS actor_name,                                            +
             |     MIN(t.title) AS hero_movie                                            +
             | FROM                                                                      +
             |     cast_info AS ci,                                                      +
             |     keyword AS k,                                                         +
             |     movie_keyword AS mk,                                                  +
             |     name AS n, title AS t                                                 +
             | WHERE                                                                     +
             |     k.keyword in ('superhero', 'sequel', 'second-part', 'marvel-comics',  +
             |                   'based-on-comic', 'tv-special', 'fight', 'violence') AND+
             |     n.name LIKE '%Downey%Robert%' AND                                     +
             |     t.production_year > 2000 AND                                          +
             |     k.id = mk.keyword_id AND                                              +
             |     t.id = mk.movie_id AND                                                +
             |     t.id = ci.movie_id AND                                                +
             |     ci.movie_id = mk.movie_id AND                                         +
             |     n.id = ci.person_id;
(2 rows)
```

The most useful settings are `learn_aqo` and `use_aqo`. In the example pattern
above, if you want to freeze the plan and prevent aqo from further learning
from queries execution statistics (which is not recommended, especially
if the data tends to change significantly), you can do
`UPDATE SET aqo_learn=false WHERE query_hash = <query_hash>;`
before commit.

The extension includes two GUC's to display the executed cardinality predictions for a query.
The `aqo.show_details = 'on'` (default - off) allows to see the aqo cardinality prediction results for each node of a query plan and an AQO summary.
The `aqo.show_hash = 'on'` (default - off) will print hash signature for each plan node and overall query. It is system-specific information and should be used for situational analysis.

The more detailed reference of AQO settings mechanism is available further.

## Advanced tuning

AQO has two kind of settings: per-query-type settings are stored in
`aqo_queries` table in the database and also there is GUC variable `aqo.mode`.

If `aqo.mode = 'disabled'`, AQO is disabled for all queries, so PostgreSQL use
its own cardinality estimations during query optimization.
It is useful if you want to disable aqo for all queries temporarily in the
current session or for the whole cluster
but not to remove or to change collected statistics and settings.

Otherwise, if the normalized query hash is stored in `aqo_queries`, AQO uses
settings from there to process the query.

Those settings are:

`Learn_aqo` setting shows whether AQO collects statistics for next execution of
the same query type. Enabled value may have computational overheads,
but it is essential when AQO model does not fit the data. It happens at the
start of AQO for the new query type or when the data distribution in database
is changed.

`Use_aqo` setting shows whether AQO cardinalities prediction be used for next
execution of such query type. Disabling of AQO usage is reasonable for that
cases in which query execution time increases after applying AQO. It happens
sometimes because of cost models incompleteness.

`fs` setting is for extra advanced AQO tuning. It may be changed manually
to optimize a number of queries using the same model. It may decrease the
amount of memory for models and even the query execution time, but also it
may cause the bad AQO's behavior, so please use it only if you know exactly
what you do.

`Auto_tuning` setting identifies whether AQO tries to tune learn_aqo and use_aqo
settings for the query on its own.

If the normalized query hash is not stored in aqo_queries, AQO behaviour depends
on the `aqo.mode`.

If `aqo.mode` is `'controlled'`, the unknown query is just ignored, i. e. the
standard PostgreSQL optimizer is used and the query execution statistics is
ignored.

If `aqo.mode` is `'learn'`, then the normalized query hash appends to aqo_queries
with the default settings `learn_aqo=true`, `use_aqo=true`, `auto_tuning=false`, and
`fs = queryid` which means that AQO uses separate machine learning
model for this query type optimization. After that the query is processed as if
it already was in aqo_queries.

`Aqo.mode = 'intelligent'` behaves similarly. The only difference is that default
`auto_tunung` variable in this case is `true`.

if `aqo.mode` is `'forced'`, the query is not appended to `aqo_queries` table, but uses
special `COMMON` feature space with identificator `fspace=0` for the query
optimization and update `COMMON` machine learning model with the execution
statistics of this query.

## Comments on AQO modes

`'controlled'` mode is the default mode to use in production, because it uses
standard PostgreSQL optimizer for all unknown query types and uses
predefined settings for the known ones.

`'learn'` mode is a base mode necessary to memorize new normalized query. The usage
pattern is follows
```
SET aqo.mode='learn'
<query>
SET aqo.mode='controlled';
<query>
<query>
...
-- unitl convergence
```

`'learn'` mode is not recommended to be used permanently for the whole cluster,
because it enables AQO for every query type, even for those ones that don't need
it, and that may lead to unnecessary computational overheads and performance
degradation.

`'intelligent'` mode is the attempt to do machine learning optimizations completelly
automatically in a self-tuning manner, i.e. determine for which queries it is
reasonable to use machine learing models and for which it is not. If you want to
rely completely on it, you may use it on per-cluster basis: just add line
`aqo.mode = 'intelligent'` into your postgresql.conf.
Nevertheless, it may still work not very good, so we do not recommend to use it
for production.

For handling workloads with dynamically generated query structures the forced
mode `aqo.mode = 'forced'` is provided.
We cannot guarantee overall performance improvement with this mode, but you
may try it nevertheless.
On one hand it lacks of intelligent tuning, so the performance for some queries
may even decrease, on the other hand it may work for dynamic workload and consumes
less memory than the `'intelligent'` mode.

## Recipes

If you want to freeze optimizer's behavior (i. e. disable learning under
workload), use

`UPDATE aqo_queries SET learn_aqo=false, auto_tuning=false;`.

If you want to disable AQO for all queries, you may use

`UPDATE aqo_queries SET use_aqo=false, learn_aqo=false, auto_tuning=false;`.

If you want to disable aqo for all queries temporarily in the current session
or for the whole cluster
but not to remove or to change collected statistics and settings,
you may use disabled mode:

`SET aqo.mode = 'disabled';`

or

`ALTER SYSTEM SET aqo.mode = 'disabled'`.

## Limitations

Note that the extension doesn't work with any kind of temporary objects, because
in query normalization AQO uses the inner OIDs of objects, which are different
for dynamically generated objects, even if their names are equal. That is why
`'intelligent'`, `'learn'` and `'forced'` aqo modes cannot be used as the system setting
with such objects in the workload. In this case you can use `aqo.mode='controlled'`
and use another `aqo.mode` inside the transaction to store settings for the queries
without temporary objects.

The extension doesn't collect statistics on replicas, because replicas are
read-only. It may use query execution statistics from master if the replica is
binary, nevertheless. The version which overcomes the replica usage limitations
is comming soon.

`'learn'` and `'intelligent'` modes are not supposed to work on per-cluster basis
with queries with dynamically generated structure, because they memorize all
normalized query hashes, which are different for all queries in such workload.
Dynamically generated constants are okay.

## License

© [Postgres Professional](https://postgrespro.com/), 2016-2022. Licensed under
[The PostgreSQL License](LICENSE).

## Reference

The paper on the proposed method is also under development, but the draft version
with experiments is available [here](https://arxiv.org/abs/1711.08330).
