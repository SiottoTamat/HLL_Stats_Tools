import json, glob
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import sessionmaker
from pathlib import Path
from dotenv import load_dotenv
import os

from models import Base, Event, ProcessedFile  # Keep ProcessedFile for tracking

# Load env vars
load_dotenv(".env")
sql_database = os.getenv("sql_database")
log_folder = Path(os.getenv("out_folder_historical_logs"))

# SQLite variable limit and batch size
SQLITE_MAX_VARS = 999
BATCH_SIZE = 50

# Create engine and set pragmas for bulk load
engine = create_engine(sql_database, echo=False)
with engine.begin() as conn:
    conn.execute(text("PRAGMA journal_mode = OFF"))
    conn.execute(text("PRAGMA synchronous = OFF"))
    conn.execute(text("PRAGMA temp_store = MEMORY"))
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_type
      ON events(type)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_event_time
      ON events(event_time)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_creation_time
      ON events(creation_time)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_inserted_at
      ON events(inserted_at)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_player1_id
      ON events(player1_id)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_player2_id
      ON events(player2_id)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_server
      ON events(server)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_weapon
      ON events(weapon)
    """
        )
    )

    # composite indexes for common multi-column filters
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_player1_time
      ON events(player1_id, event_time)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_player2_time
      ON events(player2_id, event_time)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_type_time
      ON events(type, event_time)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_type_player1_time
      ON events(type, player1_id, event_time)
    """
        )
    )
    conn.execute(
        text(
            """
      CREATE INDEX IF NOT EXISTS idx_events_type_player2_time
      ON events(type, player2_id, event_time)
    """
        )
    )

# Create tables
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine, autoflush=False)


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def ingest_batch(file_paths: list[Path], session):
    """
    Ingests a batch of JSON files into the events table.
    Skips files already in processed_files, bulk-inserts all new events,
    then marks those files processed.
    """
    mappings = []
    to_mark = []

    # Load and prepare mappings for all files in this batch
    for path in file_paths:
        fname = path.name
        if session.get(ProcessedFile, fname) is not None:
            print(f"Skipping already-processed file: {fname}")
            continue
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for ev in data:
            mappings.append(
                {
                    "event_id": ev["id"],
                    "creation_time": parse_iso(ev["creation_time"]),
                    "event_time": parse_iso(ev["event_time"]),
                    "type": ev["type"],
                    "player1_name": ev.get("player1_name"),
                    "player1_id": ev.get("player1_id"),
                    "player2_name": ev.get("player2_name"),
                    "player2_id": ev.get("player2_id"),
                    "raw": ev.get("raw"),
                    "content": ev.get("content"),
                    "server": ev.get("server"),
                    "weapon": ev.get("weapon"),
                }
            )
        to_mark.append(fname)

    if not mappings:
        return

    # Bulk insert all mappings in chunks to respect param limit
    cols = len(mappings[0])
    max_rows = max(1, SQLITE_MAX_VARS // cols)

    for i in range(0, len(mappings), max_rows):
        chunk = mappings[i : i + max_rows]
        stmt = sqlite_insert(Event.__table__).values(chunk)
        stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])
        session.execute(stmt)

    # Mark files as processed
    for fname in to_mark:
        session.add(ProcessedFile(filename=fname))

    print(f"Batch ingested: {len(to_mark)} files, {len(mappings)} events queued.")


if __name__ == "__main__":
    session = Session()
    all_files = sorted(log_folder.glob("*.json"))

    # Process in batches of BATCH_SIZE files
    for idx in range(0, len(all_files), BATCH_SIZE):
        batch = all_files[idx : idx + BATCH_SIZE]
        with session.begin():
            ingest_batch(batch, session)
        print(
            f"Committed batch {idx//BATCH_SIZE + 1} of {((len(all_files)-1)//BATCH_SIZE)+1}"
        )

    session.close()
    print("All done.")
