#!/usr/bin/python3

import os, sys, time, glob, subprocess
from db_interface import *
from configuration import *

class transferConsumer:

    def __init__(self):
        status = miConf.checkVOMSProxy()
        if status == -1:
            print("Alert - please renew proxy!")
        if status == -2:
            print("Alert - data transfers will stop soon!")
        self.di = mUtils()

    def transferToPPDdCache(self):
        # Get the list of files to transfer
        filesToTransfer = []
        for disk in miConf.disks:
            mlfn = self.di.s.query(mig_db).filter(mig_db.migDCacheStatus=="No",
                mig_db.migDisk==disk).all()
            filesToTransfer.extend(mlfn)
            print(disk, len(mlfn))
        kount = 0
        for file in filesToTransfer:
            kount = kount + 1
            self.transferOneFile(file)
            if kount >= 10: break

    def transferOneFile(self, mf):
        # Actually do the transfer for each file
        lfn = mf.migFile
        comm = f"python3 ./doTheTransfer {lfn}"
        runComm = subprocess.Popen(comm, shell=True, close_fds=True)
        theInfo = runComm.communicate()#[0].strip()
        print(runComm.returncode)
