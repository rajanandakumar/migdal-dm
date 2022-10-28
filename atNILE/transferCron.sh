#!/usr/bin/bash

ldir=$( dirname -- "$( readlink -f -- "$0"; )"; )
echo $ldir

(
    flock -n 200 || exit 1
    # commands executed under lock 
    source $ldir/../dTransfer/bin/activate;
    cd $ldir;
    echo "Running the jobs";
    python3 $ldir/transfer.py;
    deactivate;
) 200> $ldir/migLockFile2

# lock_fd=9
# exec {lock_fd}>$ldir/migLockFile2
# flock -x "$lock_fd"      # pass that FD number to flock
# cd $ldir;
# echo "Running the jobs";
# exec $lock_fd>&-         # later: release the lock


# (
#     /usr/bin/flock -n 9 || echo "Locked exiting" && exit 1;
#     # source $ldir/../dTransfer/bin/activate;
#     cd $ldir;
#     echo "Running the jobs";
#     # python3 $ldir/transfer.py;
#     # deactivate;
# ) 9>$ldir/migLockFile
