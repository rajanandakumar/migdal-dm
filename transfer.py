#!/usr/bin/python3

import transferManager
tm = transferManager.transferManager()

# First up - check if there is a disk to be transferred
status = tm.checkDiskFlag()
# aaa = tm.adler32sum("/data4/convert_DAQ_files.sh~")
# print(aaa)
# sys.exit()
if status[0] == 0:
    # We have a full disk to transfer
    dFull = status[1]
    # First - check the status of the proxy
    status = tm.checkVOMSProxy()
    if status != 0:
        print("Alert - please renew proxy!")
    # Transfer the disk contents to dCache
    # tm.writeTransferList(dFull)
    tm.transferToPPDdCache()
