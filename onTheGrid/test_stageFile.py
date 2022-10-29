#!/usr/bin/python3
import os, sys, time, subprocess
import fts3.rest.client as fsubmit
import fts3.rest.client.easy as fts3
sys.path.append("..")
from configuration import *

# Submit a test transfer to stage a file from Antares

tapeFile = "root://antares.stfc.ac.uk/eos/antares/prod/migdal/MIG_PMT_SPE_220815T161352.TEST/MIG_PMT_SPE_220815T161352.TEST.0000.dat.gz" 
zFile = "root://mover.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/gridpp/migdal-new/test/MIG_PMT_SPE_220815T161352.TEST/deleteme.dat.gz"

transf = fts3.new_transfer(tapeFile, zFile)
job = fts3.new_job(transfers=[transf], overwrite=True, verify_checksum=True, bring_online=3600, reuse=False, retry=5)
context = fts3.Context(miConf.ftsServ)
ftsJobID = fts3.submit(context, job, delegation_lifetime=fts3.timedelta(hours=72))
