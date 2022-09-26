#!/usr/bin/python3

import os, sys, time, glob, subprocess
from db_interface import *
import zlib

class transferManager:
    magicStart = "ReadyToTransfer"
    magicFinish = "ReadyForData"
    disks = ["data1", "data2", "data3", "data4"]

    # Stuff for PPD dCache
    # protocolPPD = "gsiftp"
    protocolPPD = "xroot"
    destPPD = "mover.pp.rl.ac.uk"
    pathPPD = "/pnfs/pp.rl.ac.uk/data/gridpp/migdal/test"
    dCachePath = protocolPPD + "://" + destPPD + pathPPD

    # Stuff for Tier-1 Antares
    protocolAnt = "xroot"
    destAnt = "antares.stfc.ac.uk"
    pathAnt = "/eos/antares/prod/migdal"
    antPath = protocolAnt + "://" + destAnt + pathAnt

    s = doTheSQLiteAndGetItsPointer()

    def __init__(self):
        # Get it out of the way
        status = self.checkVOMSProxy()
        if status != 0:
            print("Alert - please renew proxy!")

    def checkVOMSProxy(self):
        cmd = "voms-proxy-info --timeleft"
        timeleft = 0
        for i in os.popen(cmd):
            timeleft = int(i)

        if timeleft < 3600*24*2: # 2 days ...
            min, sec = divmod(timeleft, 60)
            hour, min = divmod(min, 60)
            print(f"Time left : {hour}h, {min}m, {sec}s")
            return -1
        return 0

    def checkDiskFlag(self):
        for disk in self.disks:
            fMagic = "/" + disk + "/" + self.magicStart
            if os.path.isfile(fMagic):
                print("Found magic file", fMagic)
                return(0, disk)
            return(0, disk)
        print("No magic file found. Back to sleep")
        # return(1, 0)

    def isFileInDB(self, lfn):
        mlfn = self.s.query(mig_db).filter(mig_db.migFile==lfn).all()
        if len(mlfn) == 1:
            return mlfn[0] # It is a list
        else:
            if len(mlfn) > 1:
                print(f"Strange error ... {mlfn}")
            return False

    def addFileToDB(self, tFile, lfn, disk, dCacheStatus="No", dCacheTime="-1",
        AntStatus="No", AntTime="-1", AntFTSID="-1", MigStatus="No"):
        newFile = mig_db(
            migFile = lfn,
            migDisk = disk,
            migTime = time.ctime(os.path.getmtime(tFile)),
            migChkSum = self.adler32sum(tFile),
            migDCacheStatus = dCacheStatus,
            migDCacheTime = dCacheTime,
            migAntStatus = AntStatus,
            migAntTime = AntTime,
            migAntFTSID = AntFTSID,
            migMigStatus = MigStatus)
        self.s.add(newFile)
        # self.s.commit()

    def updateFileInDB(self, mF, tFile, lfn, disk, dCacheStatus="No", dCacheTime="-1",
        AntStatus="No", AntTime="-1", AntFTSID="-1", MigStatus="No"):
        # self.s.update(newFile)
        mlfn = self.s.query(mig_db).filter_by(migFile=lfn)
        mlfn.update({
            # migFile is the key. So it is always present.
            # migDisk, migTime and migChkSum are filled in when adding the record. They do not change
            "migDCacheStatus" : dCacheStatus,
            "migDCacheTime" : dCacheTime,
            "migAntStatus" : AntStatus,
            "migAntTime" : AntTime,
            "migAntFTSID" : AntFTSID,
            "migMigStatus" : MigStatus})

    def printRecord(self, mF):
        print(mF.migFile)
        print(mF.migDisk)
        print(mF.migTime)
        print(mF.migChkSum)
        print(mF.migDCacheStatus)
        print(mF.migDCacheTime)
        print(mF.migAntStatus)
        print(mF.migAntTime)
        print(mF.migAntFTSID)
        print(mF.migMigStatus)

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
            kount += 1
            lfn = tFile.split(disk)[1]

            dbRec = self.isFileInDB(lfn)
            if not dbRec:
                self.addFileToDB(tFile, lfn, disk)
            else:
                print("ERROR - file already exists. Duplicate? How?")
                # self.updateFileInDB(dbRec, tFile, lfn, disk)

            destLFN = self.dCachePath + lfn
            if kount > 2000: break
            if kount %50 == 0:
                print(f"+{kount}")
                self.s.commit()
        print(f"Finally - +{kount}")
        self.s.commit()

    def adler32sum(self, filepath):
        BLOCKSIZE = 256*1024*1024
        asum = 1
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BLOCKSIZE)
                if len(chunk) > 0:
                    asum = zlib.adler32(chunk, asum)
                else:
                    break
        f.close()
        return str(hex(asum))[2:]

    def transferToPPDdCache(self):
        # Get the list of files to transfer
        filesToTransfer = []
        for disk in self.disks:
            mlfn = self.s.query(mig_db).filter(mig_db.migDCacheStatus=="No",
                mig_db.migDisk==disk).all()
            filesToTransfer.extend(mlfn)
            print(disk, len(mlfn))
        kount = 0
        for file in filesToTransfer:
            kount = kount + 1
            self.transferOneFile(file)
            if kount >= 2: break

    def transferOneFile(self, mf):
        # Actually do the transfer for each file
        lfn = mf.migFile
        cksum = mf.migChkSum
        loclLFN = "/" + mf.migDisk + lfn
        destLFN = self.dCachePath + lfn
        tapeLFN = self.antPath + lfn
        comm = f"python3 ./doTheTransfer {loclLFN} {destLFN} {tapeLFN} {cksum}"
        runComm = subprocess.Popen(comm, shell=True, close_fds=True)
        theInfo = runComm.communicate()#[0].strip()
        print(runComm.returncode)
