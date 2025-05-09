from pathlib import Path
import sys

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import json
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser
from sqlalchemy import create_engine, text, func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import sessionmaker, Session
from pathlib import Path
from dotenv import load_dotenv
import os


from hll_stats_tools.sql_tools.utils import calc_player_stats
from hll_stats_tools.sql_tools.models import (
    Base,
    Event,
    ProcessedFile,
    Game,
    Player,
    PlayerName,
    game_players,
    GameAnalysis,
    PlayerAnalysis,
)  # <-- import Game

# Load env vars
load_dotenv(".env")
sql_database = os.getenv("sql_database")
print(">> Using database at:", sql_database)
log_folder = Path(os.getenv("out_folder_historical_logs"))

# SQLite variable limit and batch size
SQLITE_MAX_VARS = 999
BATCH_SIZE = 50


def parse_datetime(s: str) -> datetime:
    # robustly handle ISO and other formats
    return dateutil_parser.isoparse(s)


def now_utc() -> datetime:
    # always aware UTC
    return datetime.now(timezone.utc)


def update_player(session, ev, id_key, name_key):
    """
    Upsert the Player record for ev[id_key], tracking name changes.
    id_key is 'player1_id' or 'player2_id'
    name_key is 'player1_name' or 'player2_name'
    """
    pid = ev.get(id_key)
    pname = ev.get(name_key) or "<unknown>"

    # If there’s no ID, nothing to do
    if not pid:
        return

    # Try to load existing player
    p = session.get(Player, pid)

    # 1) New player
    if p is None:
        p = Player(
            player_id=pid,
            current_name=pname,
            first_seen=datetime.now(),
            last_seen=datetime.now(),
        )
        session.add(p)
        session.flush()  # push so p exists in DB

        # Record the very first name
        session.add(
            PlayerName(
                player_id=pid,
                name=pname,
                changed_at=datetime.now(),
            )
        )

    # 2) Name change
    elif p.current_name != pname:
        p.current_name = pname
        p.last_seen = datetime.now()
        session.add(p)

        # Log the alias change
        session.add(
            PlayerName(
                player_id=pid,
                name=pname,
                changed_at=datetime.now(),
            )
        )

    # 3) Existing player, same name—just update last_seen
    else:
        p.last_seen = datetime.now()
        session.add(p)


def create_player_analysis(
    session: Session, stats: dict, game: Game, player: Player, test: bool = False
) -> PlayerAnalysis:
    """
    Create and persist a PlayerAnalysis record based on computed stats.

    Parameters:
    - session: active SQLAlchemy session
    - stats: dictionary of player statistics (output of calc_player_stats)
    - game: Game instance this analysis belongs to
    - player: Player instance for whom stats were computed

    Returns:
    - The newly created PlayerAnalysis instance (not yet committed)
    """
    # Instantiate a new PlayerAnalysis object with scalar fields
    new_analysis = PlayerAnalysis(
        player_id=player.player_id,
        tot_kills=stats["tot_kills"],
        tot_deaths=stats["tot_deaths"],
        tot_team_kills=stats["tot_team_kills"],
        tot_team_deaths=stats["tot_team_deaths"],
        kpm=stats["kpm"],
        dpm=stats["dpm"],
        ratio=stats["ratio"],
        time_played_secs=stats.get("time_played_seconds", 0),
        # Store distributions as JSON-encoded strings
        kill_distribution=json.dumps(stats["kill_distribution"]),
        death_distribution=json.dumps(stats["death_distribution"]),
        team_kill_distribution=json.dumps(stats["team_kill_distribution"]),
        team_death_distribution=json.dumps(stats["team_death_distribution"]),
        weapons_kill_distribution=json.dumps(stats["weapons_kills"]),
        weapons_death_distribution=json.dumps(stats["weapons_deaths"]),
    )

    # Add to session and flush to assign an ID
    if not test:
        session.add(new_analysis)
        # session.flush()
    return new_analysis


def create_analysis(
    session: Session, game: Game, skip_seeding: bool = True, test: bool = False
) -> GameAnalysis | None:
    """
    Orchestrate the full analysis process for a single game:
     1. Optionally skip seeded or unfinished games
     2. Compute per-player stats and persist them
     3. Create a GameAnalysis record linking all PlayerAnalysis entries

    Parameters:
    - session: active SQLAlchemy session
    - game: Game instance to analyze
    - skip_seeding: if True, do not analyze games marked as seeded

    Returns:
    - The newly created GameAnalysis instance, or None if skipped
    """
    # Skip if game is seeded
    if skip_seeding and game.seeding:
        return None
    # Skip if game hasn't ended
    if not game.ended:
        return None

    # Compute and persist stats for each player in the game
    player_stats_objs = []
    for player in game.players:
        stats = calc_player_stats(game, player.player_id)
        if stats is None:
            # skip players with invalid stats
            continue
        pa = create_player_analysis(session, stats, game, player, test=test)
        player_stats_objs.append(pa)

    # Create the GameAnalysis row, linking to game and player analyses
    db_analysis = GameAnalysis(
        game_key=game.game_key, game=game, player_stats=player_stats_objs
    )

    # Add, flush, and return
    if not test:
        session.add(db_analysis)
        session.flush()
    return db_analysis


def ingest_batch(
    file_paths: list[Path],
    session,
    last_nums: dict[str, int],
    active_games: dict[str, Game],
    verbose: bool = False,
):
    """
    Ingests a batch of JSON files into the events table,
    creating/closing Game rows on MATCH START/ENDED, and
    tagging each Event with its game_key.
    """
    mappings = []
    to_mark = []

    for path in file_paths:
        fname = path.name
        if session.get(ProcessedFile, fname) is not None:
            print(f"Skipping already-processed file: {fname}")
            continue

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        # Sort each file’s events by event_time so START comes before its events then END
        for ev in sorted(data, key=lambda r: r["event_time"]):
            update_player(session, ev, "player1_id", "player1_name")
            update_player(session, ev, "player2_id", "player2_name")

            ev_time = parse_datetime(ev["event_time"])
            ev_type = ev["type"]
            srv = ev.get("server")

            if srv in active_games and "THANK YOU FOR SEEDING" in (
                ev.get("content") or ""
            ):
                active_games[srv].seeding = True
                session.add(active_games[srv])

            # ——— MATCH START: open a new Game on this server ———
            if ev_type == "MATCH START":
                # bump the per-server counter
                n = (last_nums.get(srv, 0) or 0) + 1
                last_nums[srv] = n

                key = f"{srv}_{n}"

                # parse map & mode out of content, e.g. "MATCH START CARENTAN Warfare"
                # strip the leading prefix
                prefix = "MATCH START "
                raw = ev.get("content", "") or ""
                if raw.startswith(prefix):
                    after = raw[len(prefix) :]  # "ST MARIE DU MONT Warfare"
                    try:
                        game_map, game_mode = after.rsplit(
                            " ", 1
                        )  # split on last space
                    except ValueError:
                        # fallback if there’s no space
                        game_map, game_mode = after, None
                else:
                    game_map = game_mode = None

                game = Game(
                    game_key=key,
                    server=srv,
                    game_number=n,
                    start_time=ev_time,
                    map=game_map,
                    mode=game_mode,
                )
                session.add(game)
                session.flush()  # populate game_key PK
                active_games[srv] = game

            # ——— MATCH ENDED: close the existing Game ———
            elif ev_type == "MATCH ENDED" and srv in active_games:
                game = active_games[srv]
                game.end_time = ev_time
                game.ended = True

                delta = (game.end_time - game.start_time).total_seconds()
                game.duration = int(delta)
                tokens = ev["content"].split("(")[1][:5].split(" - ")
                game.allied_score = int(tokens[0])
                game.axis_score = int(tokens[1])
                game.winner = (
                    "allies" if game.allied_score > game.axis_score else "axis"
                )

                session.add(game)
                analysis = create_analysis(session, game)
                session.add(analysis)

            # ——— build the Event mapping, including game_key ———
            mappings.append(
                {
                    "event_id": ev["id"],
                    "creation_time": parse_datetime(ev["creation_time"]),
                    "event_time": ev_time,
                    "type": ev_type,
                    "player1_name": ev.get("player1_name"),
                    "player1_id": ev.get("player1_id"),
                    "player2_name": ev.get("player2_name"),
                    "player2_id": ev.get("player2_id"),
                    "raw": ev.get("raw"),
                    "content": ev.get("content"),
                    "server": srv,
                    "weapon": ev.get("weapon"),
                    # attach to current open game (if any)
                    "game_key": (
                        active_games[srv].game_key if srv in active_games else None
                    ),
                }
            )
            if srv in active_games:
                gk = active_games[srv].game_key
                for pid in (ev.get("player1_id"), ev.get("player2_id")):
                    if pid:
                        stmt = (
                            sqlite_insert(game_players)
                            .values(
                                game_key=gk,
                                player_id=pid,
                            )
                            .prefix_with("OR IGNORE")
                        )
                        session.execute(stmt)

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
        if verbose:
            print(f">>>Marked as processed: {fname}")

    print(f"Batch ingested: {len(to_mark)} files, {len(mappings)} events queued.")


if __name__ == "__main__":
    # 0) Load env & decide if we’re resetting
    load_dotenv(".env")
    force = os.getenv("FORCE_RESET", "").lower() in ("1", "true", "yes")
    print(f">>> FORCE_RESET = {force!r}, using database {sql_database!r}")

    # 1) Create engine
    engine = create_engine(sql_database, echo=False)

    # 2) Set SQLite pragmas
    with engine.begin() as conn:
        conn.execute(text("PRAGMA journal_mode = OFF"))
        conn.execute(text("PRAGMA synchronous = OFF"))
        conn.execute(text("PRAGMA temp_store = MEMORY"))

        # Optional drop‐and‐recreate schema
        if force:
            print(">>> Dropping all tables and indexes…")
            Base.metadata.drop_all(engine)
        print(">>> Creating tables and indexes…")
        Base.metadata.create_all(engine)

    # 4) Prepare a single session for all batches
    SessionLocal = sessionmaker(bind=engine, autoflush=False)
    session = SessionLocal()

    # Initialize per-server counters and open games
    last_nums = dict(
        session.query(Game.server, func.max(Game.game_number))
        .group_by(Game.server)
        .all()
    )
    active_games = {
        g.server: g for g in session.query(Game).filter(Game.ended == False).all()
    }
    print(">>> Start ingest")
    # 5) Batch‐process your JSON files
    all_files = sorted(log_folder.glob("*.json"))
    for idx in range(0, len(all_files), BATCH_SIZE):
        batch = all_files[idx : idx + BATCH_SIZE]

        ingest_batch(batch, session, last_nums, active_games, verbose=True)
        session.commit()

        print(
            f"Committed batch {idx//BATCH_SIZE + 1} of "
            f"{((len(all_files) - 1)//BATCH_SIZE) + 1}"
        )

    # add analysis for all games

    # 6) Tear down
    session.close()
    print("All done.")
