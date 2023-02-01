#!/bin/bash
ulimit -c unlimited

# Kill all orphan processes
pkill -U `whoami` -9 -e postgres
pkill -U `whoami` -9 -e pgbench
pkill -U `whoami` -9 -e psql

sleep 1

M=`pwd`/PGDATA
U=`whoami`

rm -rf $M || true
mkdir $M
rm -rf logfile.log || true

export LC_ALL=C
export LANGUAGE="en_US:en"
initdb -D $M --locale=C

# PG Version-specific settings
ver=$(pg_ctl -V | egrep -o "[0-9]." | head -1)
echo "PostgreSQL version: $ver"
if [ $ver -gt 13 ]
then
  echo "compute_query_id = 'regress'" >> $M/postgresql.conf
fi

# Speed up the 'Join Order Benchmark' test
echo "shared_buffers = 1GB" >> $M/postgresql.conf
echo "work_mem = 128MB" >> $M/postgresql.conf
echo "fsync = off" >> $M/postgresql.conf
echo "autovacuum = 'off'" >> $M/postgresql.conf

# AQO preferences
echo "shared_preload_libraries = 'aqo, pg_stat_statements'" >> $M/postgresql.conf
echo "aqo.mode = 'disabled'" >> $M/postgresql.conf
echo "aqo.join_threshold = 0" >> $M/postgresql.conf
echo "aqo.force_collect_stat = 'off'" >> $M/postgresql.conf
echo "aqo.fs_max_items = 10000" >> $M/postgresql.conf
echo "aqo.fss_max_items = 20000" >> $M/postgresql.conf

pg_ctl -w -D $M -l logfile.log start
createdb $U
psql -c "CREATE EXTENSION aqo;"
psql -c "CREATE EXTENSION pg_stat_statements"
