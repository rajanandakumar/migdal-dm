#!/usr/bin/python3

import os, sys, time, glob, shutil, subprocess
from threading import Thread

sys.path.append("..")
from db_interface import *
from configuration import *


class transferConsumer:

    filesToTransfer = []

    def __init__(self):
        status = miConf.checkVOMSProxy()
        if status == -1:
            print("Alert - please renew proxy!")
        if status == -2:
            print("Alert - data transfers will stop soon!")
        self.di = mUtils()
        self.maxThreads = miConf.maxTransferThreads
        self.transfersDirty = False

    def transferToPPDdCache(self, disk="", cleanUp=False):
        # Get the list of files to transfer
        if len(disk) > 0:
            print(f"Looking for files in {disk}")
            self.getFilesToTransfer(disk)
        else:
            print(f"Looking for files in all disks")
            for ldisk in miConf.disks:
                print(f"-- looking at disk {ldisk}")
                self.getFilesToTransfer(ldisk)
        if len(self.filesToTransfer) <= 0:
            print("No files to transfer to PPD dCache.")
            return

        print(
            f"Looking at transferring {len(self.filesToTransfer)} files using {self.maxThreads} threads"
        )
        thList = []
        for i in range(self.maxThreads):
            thread = Thread(target=self.transferOneFile, args=())  # Define the transfer
            thList.append(thread)
        for thread in thList:
            thread.start()  # Start the transfer
        for thread in thList:
            thread.join()  # Wait until the threads finish before going forward

    def getFilesToTransfer(self, disk):
        # Query sqlite for the given disk
        mlfn = (
            self.di.s.query(mig_db)
            .filter(mig_db.migDCacheStatus == "No", mig_db.migDisk == disk)
            .all()
        )
        self.filesToTransfer.extend(mlfn)
        print(f"Disk {disk} has {len(mlfn)} files to transfer")

    def transferOneFile(self):
        while len(self.filesToTransfer) > 0:
            # pick up one file to transfer
            mf = self.filesToTransfer.pop()
            lfn = (
                mf.migFile
            )  # We don't want to update a sqlite object in a separate process

            comm = f'python3 ./doTheTransfer "{lfn}"'
            runComm = subprocess.Popen(comm, shell=True, close_fds=True)
            theInfo = runComm.communicate()  # Actually run the command
            if runComm.returncode != 0:
                self.transfersDirty = True
                print(f"Failed to transfer {lfn}. Try in next iteration.")
