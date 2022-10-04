#!/usr/bin/python3

# Register the files in DIRAC (Gridpp/Imperial instance) for which we need the DIRAC environment.
# For this, we need to have the DIRAC environment. So,
#
# source /cvmfs/dirac.egi.eu/dirac/bashrc_gridpp
# dirac-proxy-init -g gridpp_migdal_production

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

dm = DataManager()

prefix = "root://mover.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data"
lfn = "/gridpp/migdal-new/test/MIG_Fe55_data_220803T145447.CAL.0000.dat"
pfn = prefix + lfn
destSE = "UKI-SOUTHGRID-RALPP-disk"
# For Antares, we need to set up a separate SE in GridPP Dirac (Daniela / Simon)

replicaTuple = (lfn, pfn, destSE)
res = dm.registerReplica(replicaTuple=replicaTuple)
print(res)
