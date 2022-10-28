#!/usr/bin/python3
# Look for files to transfer in the sqlite db
# If there are un-transferred files, transfer them over
import sys, datetime

sys.path.append("..")
from configuration import *
import transferConsumer

tc = transferConsumer.transferConsumer()

# This will try to transfer all outstanding stuff in all disks.
# Use carefully, only if needed.
for disk in miConf.disks:
    print(f"Looking at dist {disk}")
    t1 = datetime.datetime.now()
    tc.transferToPPDdCache(disk=disk)
    t2 = datetime.datetime.now()
    print(f"Time to transfer data from {disk} to PPD dCache : {t2 - t1}")
print("All done!")
