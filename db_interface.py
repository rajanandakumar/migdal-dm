import os
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
