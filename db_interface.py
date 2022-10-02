import os, datetime, zlib
from sqlalchemy import Column, String, DateTime, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
flBase = "/home/ppd/nraja/scripts/Migdal/dataTransfer/"
ftsServ = "https://lcgfts3.gridpp.rl.ac.uk:8446"

# The sqlalchemy magic to get the link to the sqlite db (or create it if needed)
Base = declarative_base()
class mig_db(Base):
    __tablename__ = 'migdal_db'

    # The unzipped file
    migFile = Column("File name", String(250), primary_key=True) # Should be unique
    migSize = Column("File size", BigInteger, default=-1, nullable=False)
    migChkSum = Column("Checksum", String(20), default="", nullable=False)

    # File information after zipping
    migZipFile = Column("Zip File name", String(250), default="", nullable=False) # Should be unique
    migZipSize = Column("Zip File size", BigInteger, default=-1, nullable=False)
    migZipChkSum = Column("Zip Checksum", String(20), default="", nullable=False)

    migDisk = Column("DAQ disk", String(20), default="", nullable=False) # disk{1..4} : lfn
    migTime = Column("Time Stamp", DateTime, default=datetime.datetime(1,1,1,0,0,0), nullable=False) # for lfn
    migDCacheStatus = Column("In PPD dCache?", String(30), default="No", nullable=False) # for lfn / lfnz
    migDCacheTime = Column("PPD dCache timestamp", DateTime, default=datetime.datetime(1,1,1,0,0,0), nullable=False) # for lfnz
    migAntStatus = Column("In Antares tape?", String(30), default="No", nullable=False) # for lfnz
    migAntTime = Column("Antares timestamp", DateTime, default=datetime.datetime(1,1,1,0,0,0), nullable=False) # for lfnz
    migAntFTSID = Column("FTS ID", String(100), default="", nullable=False) # for lfnz
    migMigStatus = Column("Tape status", String(30), default="", nullable=False) # for lfnz

def doTheSQLiteAndGetItsPointer():
    sqFile = flBase + 'sqlite_dt.db'
    engine = create_engine('sqlite:///' + sqFile)
    # Should run only the first time
    if not os.path.exists(sqFile):
        Base.metadata.create_all(engine)
    Base.metadata.bind = engine
    session = sessionmaker(bind=engine)()
    return session

class mUtils:

    def __init__(self):
        self.s = doTheSQLiteAndGetItsPointer()

    def isFileInDB(self, lfn):
        mlfn = self.s.query(mig_db).filter(mig_db.migFile==lfn).all()
        if len(mlfn) == 1:
            return mlfn[0] # It is a list
        else:
            if len(mlfn) > 1:
                print(f"Strange error ... {mlfn}")
            return False

    def addFileToDB(self, tFile, lfn, disk):
        newFile = mig_db(
            migFile = lfn,
            migSize = os.path.getsize(tFile),
            migChkSum = self.adler32sum(tFile),
            migTime = datetime.datetime.fromtimestamp(os.path.getmtime(tFile)),
            migDisk = disk)
        self.s.add(newFile)
        self.s.commit()

    def updateFileInDB(self, mF, lfnz="", zsiz="", zcksum="", dCacheStatus="No",
        dCacheTime="-1", AntStatus="No", AntTime="-1", AntFTSID="-1", MigStatus="No"):
        # Only update if variable is changed.
        # migDisk, migSize, migTime and migChkSum are filled in when adding the record.
        mUpdated = {}
        if len(lfnz) > 0:
            mUpdated["migZipFile"] = lfnz
            mUpdated["migZipSize"] = zsiz
            mUpdated["migZipChkSum"] = zcksum
        if dCacheStatus != "No": mUpdated["migDCacheStatus"] = dCacheStatus
        if type(dCacheTime) != type(""): mUpdated["migDCacheTime"] = dCacheTime
        if AntStatus != "No": mUpdated["migAntStatus"] = AntStatus
        if type(AntTime) != type(""): mUpdated["migAntTime"] = AntTime
        if AntFTSID != "-1": mUpdated["migAntFTSID"] = AntFTSID
        if MigStatus != "No": mUpdated["migMigStatus"] = MigStatus

        mlfn = self.s.query(mig_db).filter_by(migFile=mF)
        mlfn.update(mUpdated)
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
