#!/usr/bin/python3

import sys

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
    tp.writeTransferList(dFull)

    # This will try to transfer all outstanding stuff in the given disk.
    tc.transferToPPDdCache(disk=dFull)

# This will try to transfer all outstanding stuff in all disks.
# Use carefully, only if needed.
# for disk in miConf.disks:
#     tc.transferToPPDdCache(disk=disk)
