#!/usr/bin/python3

import os, sys, time, glob, shutil, subprocess
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

        print(f"Looking at transferring {len(self.filesToTransfer)} files using {self.maxThreads} threads")
        for i in range(self.maxThreads):
            thread = Thread(target=self.transferOneFile, args=()) # Define the transfer
            thread.start() # Start the transfer
            thread.join() # Wait until the threads finish before going forward

        if not self.transfersDirty:
            # Transfers all finished cleanly. Clean up the disk.
            if len(disk) > 0: # Only if this is called for a specific disk
                print(f"Ready to clean disk {disk}")
                cDirs = os.listdir("/" + disk) # File / directory names to be removed
                prefix = "/" + disk + "/"
                for dirToClean in cDirs:
                    dCl = prefix + dirToClean # Full path name
                    print(f"Cleaning up {dCl}")
                    if os.path.isdir(dCl):
                        shutil.rmtree(dCl) # Remove the directory tree
                    elif os.path.isfile(dCl):
                        os.unlink(dCl)
                    else:
                        print(f"Unknown file type : {dCl}. Cannot remove?")
                    
                open(prefix + miConf.magicFinish, 'a').close() # Set the ReadyForData flag

    def getFilesToTransfer(self, disk):
        # Query sqlite for the given disk
        mlfn = self.di.s.query(mig_db).filter(mig_db.migDCacheStatus=="No",
            mig_db.migDisk==disk).all()
        self.filesToTransfer.extend(mlfn)
        print(f"Disk {disk} has {len(mlfn)} files to transfer")

    def transferOneFile(self):
        while len(self.filesToTransfer) > 0:
            # pick up one file to transfer
            mf = self.filesToTransfer.pop()
            lfn = mf.migFile # We don't want to update a sqlite object in a separate process

            comm = f"python3 ./doTheTransfer {lfn}"
            runComm = subprocess.Popen(comm, shell=True, close_fds=True)
            theInfo = runComm.communicate() # Actually run the command
            if runComm.returncode != 0:
                self.transfersDirty = True
                print(f"Failed to transfer {lfn}. Try in next iteration.")
