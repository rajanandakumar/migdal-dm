# migdal-dm

Migdal data management notes

**Prelude:**
Need a virtual environment with the following packages
```m2crypto swig build-essential M2Crypto fts3 sqlalchemy BOOST htcondor ```

- All installed using "pip install"
- For now, we just need the environment that is already available in the dTransfer directory
  - source dTransfer/bin/activate
- We also need a voms proxy
  - Create it using "voms-proxy-init --voms gridpp:/gridpp/migdal/Role=production  --valid 168:0"
  - Copy the proxy to ```someplace``` useful
  - Set the variable using "export X509_USER_PROXY=```someplace```"
- Note that this system currently uses the X509 certificate/proxy mechanism. Updating this to tokens is left as an exercise to the interested user.

**Directories** 

<u>atNILE</u> : Stuff to be run on the Migdal DAQ system. This does the following

- Look at all the disks and check for the flag that a disk is full is there
- Write the list of files to be transferred to PPD dCache to a sqlite database
- Actually transfer the files over to PPD dCache.
- Clean up the directory that has been transferred
- Simply run the following code (after the prelude above)
  - python3 transfer.py
    - This code can be happily run "quite often"
    - It is multi-threadable
    - transfer.py is quite simple to read and go over

<u>onTheGrid</u> : Stuff to be run on any one of the mercury nodes.

- Looks over the sqlite database above and picks out the files that need to be zipped and transferred to Antares
  - Submits a job for each file to the PPD condor queue to
    - zip the file
    - Submit an FTS job to transfer the file to Antares (RAL Tier-1 tape)
- Next looks over the database again to find files with FTS jobs that have not yet gone to status "FINISHED"
  - Checks the FTS monitoring for the status of the FTS job
- The process needed for this is
  - python3 submitAndUpdate.py
  - It can be run as often as needed - preferably after the previous one has been finished

**Operational procedure**

- Migdal DAQ PC:
  -  Make sure that there is a VOMS proxy available. See the prelude above and the file proxy.sh in `/opt/ppd/darkmatter/migdal/dataTransfer` on the DAQ PC.
  -  There are two shell scripts in the atNILE directory : `transferCronWriteDB.sh` and `transferCronToPPD.sh`. These need to be run at some reasonable frequency using cron - say every 5 minutes for the first and every minute for the second.
- Mercury cluster:
  - VOMS proxy needed as above
  - Run the shell script `updateCron.sh` in the onTheGrid directory. Run it say every 10 minutes or so.
  - There are a bunch of other shell scripts that can be of use, but not strictly essential for basic operation of the DAQ system. Use appropriately.
    - `moveCondorLogs.sh` : Move the HTCondor logs of the zipping grid jobs into a date specified directory, to avoid overloading the file system.
    - `cleanUpCron.sh` : Cleans up spray from crazily initialised grid jobs polluting the `/opt/ppd/darkmatter` directory
    - `checkZipping.py` : Checks that all the files in a given day have been properly zipped, using checksums.
    - `checkAntaresStatus.py` : Example showing how to check that all the files for a given day are in Antares.
    - `diracRegister.py` : Example showing how to register files with the Imperial DIRAC instance.
    - `test_stageFile.py` : Example showing how to get a file from the Antares (tape) system to RAL PPD dCache. The staging is automatically handled by FTS (in the script).
