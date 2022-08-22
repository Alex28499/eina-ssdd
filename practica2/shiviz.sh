#!/usr/bin/env bash

logfile="$1"
rm logs/"$logfile"
GoVector --log_type shiviz --log_dir . --outfile logs/"$logfile"
rm Logger_*
rm operations.log