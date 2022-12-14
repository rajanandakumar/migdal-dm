#!/usr/bin/python3
import os, sys, time, subprocess
import fts3.rest.client.easy as fts3

sys.path.append("..")
from db_interface import *
from configuration import *
import testJob

di = mUtils()

# Files to be zipped and transferred
mlfn = di.s.query(mig_db).filter(mig_db.migDCacheStatus == "Yes", mig_db.migAntFTSID == "").all()
kount = 0
cluster_id = []
for fnn in mlfn:
    kount = kount + 1
    testJob.sub["Arguments"] = fnn.migFile
    with testJob.schedd.transaction() as txn:
        cluster_id.append(testJob.sub.queue(txn))
    # Submitting 30 jobs every iteration (10 minutes?) should be more than enough
    if kount > 30:
        break
print(f"Jobs sucmitted to condor : {cluster_id}")

# Have the files been transferred to Antares?
mlfn = (
    di.s.query(mig_db)
    .filter(
        mig_db.migDCacheStatus == "Yes",
        mig_db.migZipFile != "",
        mig_db.migAntStatus == "Submitted",
        # mig_db.migAntStatus=="Yes",
    )
    .all()
)

# Has the FTS transfer to Antares finished successfully?
for fnn in mlfn:
    lfn = fnn.migFile
    ftsID = fnn.migAntFTSID
    context = fts3.Context(miConf.ftsServ)
    ftsStat = fts3.get_job_status(context, ftsID)
    if ftsStat["job_state"] == "FINISHED":
        antTime = datetime.datetime.strptime(ftsStat["job_finished"], "%Y-%m-%dT%H:%M:%S")
        di.updateFileInDB(lfn, AntStatus="Yes", AntTime=antTime)

# Has the file been migrated to tape? Clean it.
mlfn = []
if miConf.cleanUpUnzipped:
    mlfn = (
        di.s.query(mig_db)
        .filter(
            mig_db.migDCacheStatus == "Yes",
            mig_db.migAntStatus == "Yes",
            mig_db.migMigStatus == "",
        )
        .all()
    )
# If the flag "cleanUpUnzipped" is not set, mlfn is empty. So,
# cleaning will not take place.
for fnn in mlfn:
    lfn = fnn.migFile
    lfnz = fnn.migZipFile
    tapeFile = miConf.antPath + lfnz
    comm = f"gfal-xattr {tapeFile} user.status"
    runComm = subprocess.Popen(
        comm,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    theInfo = runComm.communicate()
    if theInfo[0].decode().strip() == "NEARLINE":
        # NEARLINE = on tape
        # ONLINE = on disk
        di.updateFileInDB(lfn, MigStatus="On Tape")
        # File is safe. Remove the unzipped file from dCache
        tFile = miConf.dCachePath + lfn
        comm = f"gfal-rm {tFile}"
        runComm = subprocess.Popen(
            comm,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        theInfo = runComm.communicate()
        output = theInfo[0].decode().strip().split("\t")
        if "DELETED" in output:
            print(f"File {tFile} successfully cleaned from PPD dCache")
        elif "MISSING" in output:
            print(f"File {tFile} missing. Already cleaned from PPD dCache?")
        else:
            print(f"Unknown error cleaning {tFile} from PPD dCache?")
            print(output)
