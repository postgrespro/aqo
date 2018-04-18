#!/bin/bash

(>&2 echo "Started.")
((echo "BEGIN TRANSACTION;") && (echo "SET aqo.mode='intelligent';") && (for ((i = 0; i < ${3:-20}; ++i)); do cat $1 && (echo ";"); done) && (echo "COMMIT TRANSACTION;")) | $2 >aqo_learning_log.out 2>aqo_learning_log.err
(>&2 echo "Finished.")
