#!/usr/bin/env bash

if [ "$1" = 'postgres' ]; then
	mkdir -p "$PGDATA"
	chown -R postgres "$PGDATA"
	chmod 700 "$PGDATA"

	# look specifically for PG_VERSION, as it is expected in the DB dir
	if [ ! -s "$PGDATA/PG_VERSION" ]; then
		initdb --nosync

		{ echo; echo "host all all 0.0.0.0/0 trust"; } >> "$PGDATA/pg_hba.conf"
		{ echo; echo "host replication all 0.0.0.0/0 trust"; } >> "$PGDATA/pg_hba.conf"

		cat <<-EOF >> $PGDATA/postgresql.conf
			listen_addresses='*'
			fsync = on
			shared_preload_libraries = 'aqo'
			aqo.mode = 'intelligent'
			min_parallel_table_scan_size = 0
			min_parallel_index_scan_size = 0
		EOF

		pg_ctl -D "$PGDATA" \
				-o "-c listen_addresses=''" \
				-w start

		: ${POSTGRES_USER:=postgres}
		: ${POSTGRES_DB:=$POSTGRES_USER}
		export POSTGRES_USER POSTGRES_DB

		if [ "$POSTGRES_DB" != 'postgres' ]; then
			psql -U `whoami` postgres <<-EOSQL
				CREATE DATABASE "$POSTGRES_DB" ;
			EOSQL
			echo
		fi

		if [ "$POSTGRES_USER" = `whoami` ]; then
			op='ALTER'
		else
			op='CREATE'
		fi

		psql -U `whoami` postgres <<-EOSQL
			$op USER "$POSTGRES_USER" WITH SUPERUSER PASSWORD '';
		EOSQL
		echo

		psql -U `whoami` $POSTGRES_DB -c 'CREATE EXTENSION aqo;'
		pg_ctl -D "$PGDATA" -m fast -w stop
	fi
fi

exec "$@"
