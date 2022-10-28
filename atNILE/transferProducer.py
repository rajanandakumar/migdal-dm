#!/usr/bin/python3

import os, sys, stat, time, glob, subprocess

sys.path.append("..")
from db_interface import *
from configuration import *


class transferProducer:
    def __init__(self):
        # Get it out of the way
        status = miConf.checkVOMSProxy()
        if status == -1:
            print("Alert - please renew proxy!")
        if status == -2:
            print("Alert - data transfers will stop soon! Production still okay.")
        self.di = mUtils()

    def checkDiskFlag(self):
        for disk in miConf.disks:
            fMagic = "/" + disk + "/" + miConf.magicStart
            if os.path.isfile(fMagic):
                print("Found magic file", fMagic)
                return (0, disk)
            # if disk == "data2": # Temporary hack as disk2 is full and we cannot
            #     return(0, disk) # even create an empty file!
        print("No magic file found. Back to sleep")
        return (1, 0)

    def writeTransferList(self, disk):
        print("Looking at disk : ", disk)
        # Get the list of all files to be transferred
        # Once files are in the database, track them there and transfer them over

        # Temporary hack to be removed
        subDir = []
        if disk == "data1":
            subDir = ["221025_data"]
            # subDir = [ "ITO_amp" ]
        if disk == "data2":
            subDir = ["221013_data"]
        if disk == "data4":
            subDir = ["221027_data"]
            # subDir = [ "221013_data", "darks" ]
        files = []
        for sDir in subDir:
            print(f"Looking at {disk}/{sDir} ...")
            tF = glob.glob("/" + disk + "/" + sDir + "/**/*", recursive=True)
            files.extend(tF)

        # files = glob.glob("/" + disk + "/**/*", recursive=True)
        tFiles = [_ for _ in files if _.split("\\")[0]]

        kount = 0
        for tFile in tFiles:
            if tFile.endswith(miConf.magicStart):
                continue
            if len(tFile) < 10:
                continue
            if os.path.isdir(tFile):
                continue
            if not os.access(tFile, os.R_OK):
                # os.chmod(tFile, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                print(f"File {tFile} exists but is not readable. Skipping.")
                continue
            kount += 1
            # lfn = tFile.split(disk)[1]
            lfn = self.di.addLFNDateStamp(tFile.split(disk)[1])

            dbRec = self.di.isFileInDB(lfn)
            if not dbRec:
                self.di.addFileToDB(tFile, lfn, disk)
            else:
                print("ERROR - file already exists. Duplicate? How?")

            destLFN = miConf.dCachePath + lfn
            if kount % 50 == 0:
                print(f"+{kount}")
        print(f"Finally - +{kount}")
        print(f"Removing the magic start file")
        fMagic = "/" + disk + "/" + miConf.magicStart
        try:
            os.unlink(fMagic)
        except FileNotFoundError as e:
            print(e)
            print("Some hack in place? File {miConf.magicStart} not found.")
