#!/bin/bash

# Aim : Move the logs from yesterday to a separate directory

# Get into the correct directory whereever the cron runs from
ldir=$( dirname -- "$( readlink -f -- "$0"; )"; )
cd $ldir/onTheGrid/output

# Get yesterday
end_date=`date -I`
yesterday=`date '+%C%y-%m-%d' -d "$end_date-1 days"`
mkdir -p $yesterday

# Move the logs into the "yesterday" directory
find ./ -maxdepth 1 -atime 0 | grep out | xargs -l mv -t $yesterday 