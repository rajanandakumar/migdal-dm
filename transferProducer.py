#!/usr/bin/python3

import os, sys, stat, time, glob, subprocess
from db_interface import *
from configuration import *

class transferProducer:

    def __init__(self):
        # Get it out of the way
        status = miConf.checkVOMSProxy()
        if status != 0:
            print("Alert - please renew proxy!")
        self.di = mUtils()

    def checkDiskFlag(self):
        for disk in miConf.disks:
            fMagic = "/" + disk + "/" + miConf.magicStart
            if os.path.isfile(fMagic):
                print("Found magic file", fMagic)
                return(0, disk)
            # return(0, disk)
        print("No magic file found. Back to sleep")
        return(1, 0)

    def writeTransferList(self, disk):
        print("Looking at disk : ", disk)
        # Get the list of all files to be transferred
        # Once files are in the database, track them there and transfer them over
        files = glob.glob("/" + disk + '/**/*', recursive=True)
        tFiles = [_ for _ in files if _.split("\\")[0]]

        kount = 0
        for tFile in tFiles:
            if len(tFile) < 10: continue
            if os.path.isdir(tFile): continue
            if not os.access(tFile, os.R_OK):
                # os.chmod(tFile, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                print(f"File {tFile} exists but is not readable. Skipping.")
                continue
            kount += 1
            lfn = tFile.split(disk)[1]

            dbRec = self.di.isFileInDB(lfn)
            if not dbRec:
                self.di.addFileToDB(tFile, lfn, disk)
            else:
                print("ERROR - file already exists. Duplicate? How?")

            destLFN = miConf.dCachePath + lfn
            if kount > 2000:
                print("This is just a test. So, stopping here ...")
                break
            if kount %50 == 0:
                print(f"+{kount}")
        print(f"Finally - +{kount}")
