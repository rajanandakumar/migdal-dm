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
print(datetime.datetime.now())

t1 = datetime.datetime.now()
tc.transferToPPDdCache()
t2 = datetime.datetime.now()
print(f"Time this transfer process (to dCache) to run : {t2 - t1}")

# for disk in miConf.disks:
#     print(f"Looking at disk {disk}")
#     t1 = datetime.datetime.now()
#     tc.transferToPPDdCache(disk=disk)
#     t2 = datetime.datetime.now()
#     print(f"Time to transfer data from {disk} to PPD dCache : {t2 - t1}")
print("All done!")
