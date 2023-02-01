#!/bin/bash

# ##############################################################################
#
# Make dump of a knowledge base
#
# ##############################################################################

psql -c "CREATE TABLE aqo_data_dump AS SELECT * FROM aqo_data;"
psql -c "CREATE TABLE aqo_queries_dump AS SELECT * FROM aqo_queries;"
psql -c "CREATE TABLE aqo_query_texts_dump AS SELECT * FROM aqo_query_texts;"
psql -c "CREATE TABLE aqo_query_stat_dump AS SELECT * FROM aqo_query_stat;"

pg_dump --table='aqo*' -f knowledge_base.dump $PGDATABASE

psql -c "DROP TABLE aqo_data_dump, aqo_queries_dump, aqo_query_texts_dump, aqo_query_stat_dump"

