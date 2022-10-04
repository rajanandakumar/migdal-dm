#!/usr/bin/python3
import os, sys, time, subprocess
import fts3.rest.client.easy as fts3

sys.path.append("..")
from db_interface import *
from configuration import *
import testJob

di = mUtils()

# Files to be zipped and transferred
mlfn = di.s.query(mig_db).filter(mig_db.migDCacheStatus=="Yes",
    mig_db.migAntFTSID==""
    ).all()
print(len(mlfn))
for fnn in mlfn:
    # For now transfer only 100MB or larger files
    if fnn.migSize > 100*1024*1024:
        # Submit the job to be zipped
        # comm = f"python3 zipAndGo.py {fnn.migFile}"
        time.sleep(1.0)
        testJob.sub["Arguments"] = fnn.migFile
        with testJob.schedd.transaction() as txn:
            cluster_id = testJob.sub.queue(txn)
        print(cluster_id)

# Has the file been transferred to tape?
mlfn = di.s.query(mig_db).filter(mig_db.migDCacheStatus=="Yes",
    mig_db.migZipFile!="",
    mig_db.migAntStatus=="Submitted",
    # mig_db.migAntStatus=="Yes",
    ).all()

for fnn in mlfn:
    lfn = fnn.migFile
    ftsID = fnn.migAntFTSID
    context = fts3.Context(miConf.ftsServ)
    ftsStat = fts3.get_job_status(context, ftsID)
    if ftsStat["job_state"] == "FINISHED":
        antTime = datetime.datetime.strptime(ftsStat["job_finished"], '%Y-%m-%dT%H:%M:%S')
        di.updateFileInDB(lfn, AntStatus="Yes", AntTime=antTime)
