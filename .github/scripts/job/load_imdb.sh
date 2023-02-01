#!/bin/bash

psql -f $JOB_DIR/schema.sql
psql -vdatadir="'$JOB_DIR'" -f $JOB_DIR/copy.sql

