#!/usr/bin/bash

ldir=$( dirname -- "$( readlink -f -- "$0"; )"; )
echo `date`
export X509_USER_PROXY=/opt/ppd/darkmatter/migdal/dataTransfer/proxy.proxy

(
    flock -n 203 || exit 1
    # commands executed under lock 
    source $ldir/../dTransfer/bin/activate;
    cd $ldir;
    echo "Running the jobs";
    python3 $ldir/submitAndUpdate.py;
    deactivate;
) 203> $ldir/migLockFile203
