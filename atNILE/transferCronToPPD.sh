#!/usr/bin/bash

ldir=$( dirname -- "$( readlink -f -- "$0"; )"; )
echo $ldir

(
    flock -n 202 || exit 1
    # commands executed under lock 
    source $ldir/../dTransfer/bin/activate;
    cd $ldir;
    echo "Transferring any found file to PPD dCache";
    python3 $ldir/transferToPPD.py;
    deactivate;
) 202> $ldir/migLockFile202
