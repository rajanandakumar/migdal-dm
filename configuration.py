import os


class miConf:
    # Some generic stuff - primarily configuration
    magicStart = "ReadyToTransfer"
    magicFinish = "ReadyForData"
    disks = ["data1", "data2", "data3", "data4"]
    # disks = ["data1", "data2", "data3", "data4", "data1/test_bandwidth"]

    # Flag to control if we delete unzipped file on dCache after file has been
    # migrated to tape
    maxCondorJobsInIteration = 150
    cleanUpUnzipped = False

    # Stuff for PPD dCache
    protocolPPD = "root"
    # protocolPPD = "gsiftp"
    destPPD = "mover.pp.rl.ac.uk"
    # destPPD = "heplns140.pp.rl.ac.uk"

    portPPD = ""
    if protocolPPD == "root":
        portPPD = "1094"

    if len(portPPD) > 2:
        destPPD = destPPD + ":" + portPPD

    pathPPD = "/pnfs/pp.rl.ac.uk/data/gridpp/migdal/"
    dCachePath = protocolPPD + "://" + destPPD + pathPPD
    maxTransferThreads = 5

    # Zipping algorithm and file suffix
    zipAlg = "bzip2"
    zipSuffix = ".bz2"
    # zipAlg = "gzip -9"
    # zipSuffix = ".gz"

    # Stuff for Tier-1 Antares
    protocolAnt = "root"
    destAnt = "antares.stfc.ac.uk"
    pathAnt = "/eos/antares/prod/migdal"
    antPath = protocolAnt + "://" + destAnt + pathAnt

    # FTS server we use.
    ftsServ = "https://lcgfts3.gridpp.rl.ac.uk:8446"

    # Generic utility for all processes
    def checkVOMSProxy():
        cmd = "voms-proxy-info --timeleft"
        timeleft = 0
        for i in os.popen(cmd):
            timeleft = int(i)

        if timeleft < 3600 * 24 * 2:  # 2 days ...
            min, sec = divmod(timeleft, 60)
            hour, min = divmod(min, 60)
            print(f"Time left : {hour}h, {min}m, {sec}s")
            if timeleft < 1000:  # seconds ... so 15 minutes
                print("You really need to renew the proxy.")
                print("Hint : \n voms-proxy-init --voms gridpp:/gridpp/migdal/Role=production  --valid 168:0")
                return -2
            return -1
        return 0
