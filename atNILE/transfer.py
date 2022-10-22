#!/usr/bin/python3

import sys, datetime

sys.path.append("..")
from configuration import *
import transferProducer
import transferConsumer
tp = transferProducer.transferProducer()
tc = transferConsumer.transferConsumer()

# First up - check if there is a disk to be transferred
status = tp.checkDiskFlag()

if status[0] == 0:
    # We have a full disk to transfer
    dFull = status[1]

    # First - check the status of the proxy
    status = miConf.checkVOMSProxy()
    if status != 0:
        print("Alert - please renew proxy!")

    # Write the information to the database
    t1 = datetime.datetime.now()
    tp.writeTransferList(dFull)
    t2 = datetime.datetime.now()
    print(f"Time to write to the SQLiteDBs : {t2 - t1}")

    # This will try to transfer all outstanding stuff in the given disk.
    t1 = datetime.datetime.now()
    tc.transferToPPDdCache(disk=dFull)
    t2 = datetime.datetime.now()
    print(f"Time to transfer data to PPD dCache : {t2 - t1}")

# This will try to transfer all outstanding stuff in all disks.
# Use carefully, only if needed.
# for disk in miConf.disks:
#     tc.transferToPPDdCache(disk=disk)
