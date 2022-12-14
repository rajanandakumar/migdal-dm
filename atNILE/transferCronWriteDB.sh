#!/usr/bin/bash

ldir=$( dirname -- "$( readlink -f -- "$0"; )"; )
echo $ldir

(
    flock -n 201 || exit 1
    # commands executed under lock 
    source $ldir/../dTransfer/bin/activate;
    cd $ldir;
    echo "Writing the job list to the database";
    python3 $ldir/transferWriteDB.py;
    deactivate;
) 201> $ldir/migLockFile201
