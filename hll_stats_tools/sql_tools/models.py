from sqlalchemy import Column, Integer, String, DateTime, Text, text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class Event(Base):
    __tablename__ = "events"

    event_id = Column(Integer, unique=True, primary_key=True, nullable=False)

    # log JSON has ISO-8601 timestamps.
    creation_time = Column(DateTime)
    event_time = Column(DateTime, nullable=False)

    # The type of event, e.g. "MATCH START", "KILL", etc.
    type = Column(String, nullable=False)

    # Player fields (some events only have player1, some have both).
    player1_name = Column(String)
    player1_id = Column(String)
    player2_name = Column(String)
    player2_id = Column(String)
    weapon = Column(String)

    # The raw/textual data and any content string
    raw = Column(Text)
    content = Column(Text)

    # Where this came from
    server = Column(String)

    # When we inserted into SQLite
    inserted_at = Column(DateTime, default=datetime.now, nullable=False)


class ProcessedFile(Base):
    __tablename__ = "processed_files"

    filename = Column(String, primary_key=True)
    ingested_at = Column(DateTime, default=datetime.now, nullable=False)
