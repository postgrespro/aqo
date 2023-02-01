#!/bin/bash

# ##############################################################################
#
#
# ##############################################################################

# Show error delta (Negative result is a signal of possible issue)
result=$(psql -t -c "SELECT count(*) FROM aqo_cardinality_error(true) c JOIN aqo_cardinality_error(false) o USING (id) WHERE (o.error - c.error) < 0")

if [ $result -gt 0 ]; then
  exit 1;
fi

exit 0;
