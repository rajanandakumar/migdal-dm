import os, time, zlib
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
flBase = "/home/ppd/nraja/scripts/Migdal/dataTransfer/"
ftsServ = "https://lcgfts3.gridpp.rl.ac.uk:8446"

# The sqlalchemy magic to get the link to the sqlite db (or create it if needed)
Base = declarative_base()
class mig_db(Base):
    __tablename__ = 'migdal_db'
    migFile = Column("File name", String(250), primary_key=True) # Should be unique
    migSize = Column("File size", String(20), primary_key=True) # Should be unique
    migDisk = Column("DAQ disk", String(20), primary_key=False) # disk{1..4}
    migTime = Column("Time Stamp", String(30), nullable=False)
    migChkSum = Column("Checksum", String(500), nullable=False)
    migDCacheStatus = Column("In PPD dCache?", String(10), nullable=False)
    migDCacheTime = Column("PPD dCache timestamp", String(30), nullable=False)
    migAntStatus = Column("In Antares tape?", String(10), nullable=False)
    migAntTime = Column("Antares timestamp", String(30), nullable=False)
    migAntFTSID = Column("FTS ID", String(100), nullable=False)
    migMigStatus = Column("Tape status", String(30), nullable=False)

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

    # def db_saveStuff(self):
    #     self.s.commit()

    def addFileToDB(self, tFile, lfn, disk, dCacheStatus="No", dCacheTime="-1",
        AntStatus="No", AntTime="-1", AntFTSID="-1", MigStatus="No"):
        newFile = mig_db(
            migFile = lfn,
            migSize = os.path.getsize(tFile),
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
        self.s.commit()

    def updateFileInDB(self, mF, dCacheStatus="No", dCacheTime="-1",
        AntStatus="No", AntTime="-1", AntFTSID="-1", MigStatus="No"):
        mlfn = self.s.query(mig_db).filter_by(migFile=mF)
        mlfn.update({
            # migFile is the key. So it is always present.
            # migDisk, migSize, migTime and migChkSum are filled in when adding the record. They do not change
            "migDCacheStatus" : dCacheStatus,
            "migDCacheTime" : dCacheTime,
            "migAntStatus" : AntStatus,
            "migAntTime" : AntTime,
            "migAntFTSID" : AntFTSID,
            "migMigStatus" : MigStatus})
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
