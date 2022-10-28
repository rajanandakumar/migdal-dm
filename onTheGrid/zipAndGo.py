#!/usr/bin/python3
import os, sys, time, subprocess
import fts3.rest.client as fsubmit
import fts3.rest.client.easy as fts3

sys.path.append("..")
from db_interface import *
from configuration import *

def doTheCommand(comm):
    print(comm)
    runComm = subprocess.Popen(comm, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    theInfo = runComm.communicate()
    commandError = 0
    for aa in theInfo:
        if "ERROR" in str(aa) or "Error" in str(aa) or "error" in str(aa):
            commandError = 1
    return commandError

di = mUtils()
lfn = sys.argv[1] # "Logical file name"

status = miConf.checkVOMSProxy()
if status == -1:
    print("Alert - please renew proxy!")
if status == -2:
    print("Proxy too short for transfers ... aborting")
    sys.exit(-3)

mf = di.isFileInDB(lfn) # Get the record from sqlite
dFile = miConf.pathPPD + lfn
print(f"Downloading and b-zipping {dFile} from PPD dCache ...")

# Copy from dCache
di.updateFileInDB(lfn, dCacheStatus="Zipping")
fn = os.path.basename(dFile)
dn = os.path.dirname(dFile)
command = f"cp {dFile} ."
status = doTheCommand(command)
if status != 0 :
    print(f"Error getting file {dFile} from dCache. Exiting")
    sys.exit(-1)

# Zip with the algorithm defined in the configuration
command = f"{miConf.zipAlg} {fn}"
status = doTheCommand(command)
if status != 0 :
    print(f"Error zipping file {fn}? Exiting")
    sys.exit(-2)

# Zip file name, path and checksum
sFile = fn + miConf.zipSuffix # Assume name is in this format
lfnz = lfn + miConf.zipSuffix
zFile = miConf.dCachePath + lfn + miConf.zipSuffix
cksum = di.adler32sum(sFile)

# Upload the zipped file to dCache
print(f"Successfully zipped file {dFile}. Uploading {zFile} to dCache.")
di.updateFileInDB(lfn, dCacheStatus="Uploading Zip")
command = f"gfal-copy -v -f -K adler32:{cksum} -r -p --checksum-mode both"
comm = f"{command} {sFile} {zFile}"
status = doTheCommand(comm)
if status != 0 :
    print(f"Error uploading zipped file {fn}? Exiting")
    sys.exit(-2)
print(f"File {lfn} successfully in dCache")
dCacheTime = datetime.datetime.fromtimestamp(time.time())
di.updateFileInDB(lfn, lfnz=lfnz, zsiz=os.path.getsize(sFile), zcksum=cksum, dCacheStatus="Yes", dCacheTime=dCacheTime)

# Delete local copy
os.unlink(sFile)

tapeFile = miConf.antPath + lfn + miConf.zipSuffix
di.updateFileInDB(lfn, dCacheStatus="Yes", dCacheTime=time.ctime())

# Submit transfer to Antares here (works!)
transf = fts3.new_transfer(zFile, tapeFile)
job = fts3.new_job(transfers=[transf], overwrite=True, verify_checksum=True, reuse=False, retry=5) # To avoid deleted files snarling up the system for hours
context = fts3.Context(miConf.ftsServ)
ftsJobID = fts3.submit(context, job, delegation_lifetime=fts3.timedelta(hours=72))
di.updateFileInDB(lfn, AntStatus="Submitted", MigStatus="No", AntFTSID=ftsJobID)
