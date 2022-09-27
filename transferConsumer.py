#!/usr/bin/python3

import os, sys, time, glob, subprocess
from db_interface import *
from configuration import *
from threading import Thread

class transferConsumer:

    filesToTransfer = []

    def __init__(self):
        status = miConf.checkVOMSProxy()
        if status == -1:
            print("Alert - please renew proxy!")
        if status == -2:
            print("Alert - data transfers will stop soon!")
        self.di = mUtils()
        self.maxThreads = 1

    def transferToPPDdCache(self):
        # Get the list of files to transfer
        for disk in miConf.disks:
            mlfn = self.di.s.query(mig_db).filter(mig_db.migDCacheStatus=="No",
                mig_db.migDisk==disk).all()
            self.filesToTransfer.extend(mlfn)
            print(disk, len(mlfn))

        print(len(self.filesToTransfer))
        for i in range(self.maxThreads):
            # self.transferOneFile(self,file)
            thread = Thread(target=self.transferOneFile, args=())
            thread.start()


    def transferOneFile(self):
        while len(self.filesToTransfer) > 0:
            # pick up one file to transfer
            mf = self.filesToTransfer.pop()
            lfn = mf.migFile # We don't want to update a sqlite object in a separate process

            comm = f"python3 ./doTheTransfer {lfn}"
            runComm = subprocess.Popen(comm, shell=True, close_fds=True)
            theInfo = runComm.communicate() # Actually run the command
            if runComm.returncode != 0:
                print(f"Failed to transfer {lfn}. Try in next iteration.")
