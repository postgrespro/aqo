#!/bin/bash

# ##############################################################################
#
# Pass each JOB query over the DBMS instance. Use $1 to specify a number of
# iterations, if needed.
#
# Results:
# - explains.txt - explain of each query
# - job_onepass_aqo_stat.dat - short report on execution time
# - knowledge_base.dump - dump of the AQO knowledge base
#
# ##############################################################################

echo "The Join Order Benchmark 1Pass"
echo -e "Query Number\tITER\tQuery Name\tExecution Time, ms" > report.txt
echo -e "Clear a file with explains" > explains.txt

if [ $# -eq 0 ]
then
  ITERS=1
else
  ITERS=$1
fi

echo "Execute JOB with the $ITERS iterations"

filenum=1
for file in $JOB_DIR/queries/*.sql
do
  # Get filename
  short_file=$(basename "$file")

  echo -n "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) " > test.sql
  cat $file >> test.sql

  for (( i=1; i<=$ITERS; i++ ))
  do
    result=$(psql -f test.sql)
    echo -e $result >> explains.txt
    exec_time=$(echo $result | sed -n 's/.*"Execution Time": \([0-9]*\.[0-9]*\).*/\1/p')
    echo -e "$filenum\t$short_file\t$i\t$exec_time" >> report.txt
    echo -e "$filenum\t$i\t$short_file\t$exec_time"
  done
filenum=$((filenum+1))
done

# Show total optimizer error in the test
psql -c "SELECT sum(error) AS total_error FROM aqo_cardinality_error(false)"
psql -c "SELECT sum(error) AS total_error_aqo FROM aqo_cardinality_error(true)"

# Show error delta (Negative result is a signal of possible issue)
psql -c "
SELECT id, (o.error - c.error) AS errdelta
  FROM aqo_cardinality_error(true) c JOIN aqo_cardinality_error(false) o
  USING (id)
"

