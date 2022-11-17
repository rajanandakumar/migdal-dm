#!/usr/bin/python3
# Check if there are files to be transferred.
# Write the files we should transfer to the sqlite db

import sys, datetime

sys.path.append("..")
from configuration import *
import transferProducer
import transferConsumer

print(datetime.datetime.now())
tp = transferProducer.transferProducer()

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
