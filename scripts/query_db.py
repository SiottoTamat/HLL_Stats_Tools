from datetime import datetime
import os
import time
from dotenv import load_dotenv

from sqlalchemy import create_engine, distinct, func
from sqlalchemy.orm import sessionmaker

from hll_stats_tools.sql_pipeline.models import Event

# Load env vars
load_dotenv(".env")
sql_database = os.getenv("sql_database")


def main():
    engine = create_engine(sql_database, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    player_id = "steam_id"  # replace with the Steam ID you want
    start = datetime(2025, 1, 1, 0, 0, 0)
    end = datetime(2025, 2, 1, 0, 0, 0)  # up to but not including Feb 1st

    start_time = time.perf_counter()
    kill_count = (
        session.query(func.count(Event.event_id))
        .filter(
            Event.type == "KILL",
            Event.player1_id == player_id,
            Event.event_time >= start,
            Event.event_time < end,
        )
        .scalar()
    )
    death_count = (
        session.query(func.count(Event.event_id))
        .filter(
            Event.type == "KILL",
            Event.player2_id == player_id,
            Event.event_time >= start,
            Event.event_time < end,
        )
        .scalar()
    )

    end_time = time.perf_counter()

    # 1. Get all event types
    rows = session.query(distinct(Event.type)).order_by(Event.type).all()

    # 2. rows will be a list of 1‐tuples, e.g. [('DEATH',), ('JOIN',), ('KILL',), …]
    #    so unpack them for printing:
    types = [r[0] for r in rows]

    print("All event types:", types)

    print(f"Time taken: {end_time - start_time}")

    print(
        f"Player {player_id} scored {kill_count} "
        f"kills and died {death_count} times in {start.month} {start.year}"
    )
    # 4) Clean up
    session.close()


if __name__ == "__main__":
    main()
