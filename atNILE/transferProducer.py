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
            print(f"Looking at {disk}")
            fMagic = "/" + disk + "/" + miConf.magicStart
            if os.path.isfile(fMagic):
                print("Found magic file", fMagic)
                return (0, disk)
            # # Hack as we do not yet have write access in midaq directories
            # if len(disk) > 5 and len(disk.split("/")) > 1:
            #     return (0, disk)
        print("No magic file found. Back to sleep")
        return (1, 0)

    def remove_empty_folders(self, path_abs):
        walk = list(os.walk(path_abs, topdown=False))
        for path, _, _ in walk[::-1]:
            if len(os.listdir(path)) == 0:
                os.rmdir(path)


    def writeTransferList(self, disk):
        self.disk = disk
        print("Looking at disk : ", self.disk)

        # First clean up the disk of empty directories
        self.remove_empty_folders(self.disk)

        # Get the list of all files to be transferred
        # Once files are in the database, track them there and transfer them over
        if "MIG" in disk:
            files = glob.glob("/" + disk + "/**/*", recursive=True)
        else:
            files = glob.glob("/" + disk + "/MIG_*/**/*", recursive=True)
        tFiles = [_ for _ in files if _.split("\\")[0]]

        self.kount = 0
        if len(tFiles) == 0:
            print("Transfer flag set, but found no files to transfer?")
        else:
            for tFile in tFiles:
                self.checkAndAddFileToDB(tFile)

        print(f"Finally - +{self.kount}")
        # if len(self.disk) > 5 and len(self.disk.split("/")) > 1:
        #     print(f"No magic start file to remove ...")
        #     return
        print(f"Removing the magic start file")
        fMagic = "/" + self.disk + "/" + miConf.magicStart
        try:
            os.unlink(fMagic)
        except FileNotFoundError as e:
            print("Some hack in place? File {miConf.magicStart} not found.")

    def checkAndAddFileToDB(self, tFile):
        if len(tFile) < 10 or tFile.endswith(miConf.magicStart):
            return
        if os.path.isdir(tFile):
            return
        if not os.access(tFile, os.R_OK):
            # os.chmod(tFile, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            print(f"File {tFile} exists but is not readable. Skipping.")
            return

        lfn = self.di.addLFNDateStamp(tFile.split(self.disk)[1])
        dbRec = self.di.isFileInDB(lfn)

        if not dbRec:
            self.di.addFileToDB(tFile, lfn, self.disk)
        else:
            print("ERROR - file already exists. Duplicate? How?")

        self.kount = self.kount + 1
        if self.kount % 50 == 0:
            print(f"+{self.kount}")
