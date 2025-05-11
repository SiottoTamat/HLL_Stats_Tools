import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dateutil import parser as dateutil_parser
from dotenv import load_dotenv
from sqlalchemy import create_engine, false, func, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker

from hll_stats_tools.sql_pipeline.models import (
    Base,
    Event,
    Game,
    GameAnalysis,
    Player,
    PlayerAnalysis,
    PlayerName,
    ProcessedFile,
    game_players,
)
from hll_stats_tools.sql_pipeline.sql_utils import calc_player_stats
from hll_stats_tools.utils.logger_utils import setup_logger

logger = setup_logger(__name__)


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


def is_processed(fname, session):
    return session.get(ProcessedFile, fname) is not None


def parse_match_start(ev, ev_time, srv, last_nums):
    n = (last_nums.get(srv, 0) or 0) + 1
    last_nums[srv] = n
    key = f"{srv}_{n}"
    raw = ev.get("content", "") or ""
    prefix = "MATCH START "
    if raw.startswith(prefix):
        after = raw[len(prefix) :]
        try:
            game_map, game_mode = after.rsplit(" ", 1)
        except ValueError:
            game_map, game_mode = after, None
    else:
        game_map = game_mode = None
    return Game(
        game_key=key,
        server=srv,
        game_number=n,
        start_time=ev_time,
        map=game_map,
        mode=game_mode,
    )


def close_match(ev, game, ev_time):
    game.end_time = ev_time
    game.ended = True
    game.duration = int((game.end_time - game.start_time).total_seconds())
    tokens = ev["content"].split("(")[1][:5].split(" - ")
    game.allied_score = int(tokens[0])
    game.axis_score = int(tokens[1])
    game.winner = "allies" if game.allied_score > game.axis_score else "axis"


def build_event_record(ev, ev_time, active_games):
    srv = ev.get("server")
    return {
        "event_id": ev["id"],
        "creation_time": parse_datetime(ev["creation_time"]),
        "event_time": ev_time,
        "type": ev["type"],
        "player1_name": ev.get("player1_name"),
        "player1_id": ev.get("player1_id"),
        "player2_name": ev.get("player2_name"),
        "player2_id": ev.get("player2_id"),
        "raw": ev.get("raw"),
        "content": ev.get("content"),
        "server": srv,
        "weapon": ev.get("weapon"),
        "game_key": active_games[srv].game_key if srv in active_games else None,
    }


def process_event_file(data, session, last_nums, active_games):
    """
    Processes all events in a single parsed JSON file.
    Returns a list of event mappings to be inserted.
    """
    records = []

    for ev in sorted(data, key=lambda r: r["event_time"]):
        update_player(session, ev, "player1_id", "player1_name")
        update_player(session, ev, "player2_id", "player2_name")

        ev_time = parse_datetime(ev["event_time"])
        ev_type = ev["type"]
        srv = ev.get("server")

        if srv in active_games and "THANK YOU FOR SEEDING" in (ev.get("content") or ""):
            active_games[srv].seeding = True
            session.add(active_games[srv])

        if ev_type == "MATCH START":
            game = parse_match_start(ev, ev_time, srv, last_nums)
            session.add(game)
            session.flush()
            active_games[srv] = game

        elif ev_type == "MATCH ENDED" and srv in active_games:
            game = active_games[srv]
            close_match(ev, game, ev_time)
            session.add(game)
            analysis = create_analysis(session, game)
            if analysis:
                session.add(analysis)
            else:
                logger.debug(
                    "Skipped analysis for game %s (ended=%s, seeding=%s)",
                    game.game_key,
                    game.ended,
                    game.seeding,
                )

        records.append(build_event_record(ev, ev_time, active_games))

        if srv in active_games:
            gk = active_games[srv].game_key
            for pid in (ev.get("player1_id"), ev.get("player2_id")):
                if pid:
                    stmt = (
                        sqlite_insert(game_players)
                        .values(game_key=gk, player_id=pid)
                        .prefix_with("OR IGNORE")
                    )
                    session.execute(stmt)

    return records


def ingest_batch(
    file_paths, session, last_nums, active_games, verbose=False
):  # noqa: C901
    mappings = []
    to_mark = []

    for path in file_paths:
        fname = path.name
        if is_processed(fname, session):
            logger.info("Skipping already-processed file: %s", fname)
            continue

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        records = process_event_file(data, session, last_nums, active_games)
        mappings.extend(records)
        to_mark.append(fname)

    if not mappings:
        return

    cols = len(mappings[0])
    max_rows = max(1, SQLITE_MAX_VARS // cols)
    for i in range(0, len(mappings), max_rows):
        chunk = mappings[i : i + max_rows]
        stmt = sqlite_insert(Event.__table__).values(chunk)
        stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])
        session.execute(stmt)

    for fname in to_mark:
        session.add(ProcessedFile(filename=fname))
        if verbose:
            logger.info("Marked as processed: %s", fname)

    logger.info(
        "Batch ingested: %d files, %d events queued.", len(to_mark), len(mappings)
    )


def run_sql_pipeline():

    # Load env vars
    load_dotenv(".env")
    sql_database = os.getenv("sql_database")
    logger.info(">> Using database at:", sql_database)
    log_folder = Path(os.getenv("out_folder_historical_logs"))

    # 0) Load env & decide if we’re resetting
    force = os.getenv("FORCE_RESET", "").lower() in ("1", "true", "yes")
    logger.info(">>> FORCE_RESET = %r, using database %r", force, sql_database)
    if force:
        logger.warning("FORCE_RESET is set — this will drop all tables and indexes.")
        confirm = input("Are you sure? [y/N] ").strip().lower()
        if confirm not in ("y", "yes"):
            logger.info("Aborting.")
            return

    # 1) Create engine
    engine = create_engine(sql_database, echo=False)

    # 2) Set SQLite pragmas
    with engine.begin() as conn:
        conn.execute(text("PRAGMA journal_mode = OFF"))
        conn.execute(text("PRAGMA synchronous = OFF"))
        conn.execute(text("PRAGMA temp_store = MEMORY"))

        # Optional drop‐and‐recreate schema
        if force:
            logger.info("Dropping all tables and indexes…")
            Base.metadata.drop_all(engine)
        logger.info("Creating tables and indexes…")
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
        g.server: g for g in session.query(Game).filter(Game.ended == false()).all()
    }
    logger.info("Start ingest")
    # 5) Batch‐process your JSON files
    all_files = sorted(log_folder.glob("*.json"))
    for idx in range(0, len(all_files), BATCH_SIZE):
        batch = all_files[idx : idx + BATCH_SIZE]

        ingest_batch(batch, session, last_nums, active_games, verbose=True)
        session.commit()

        logger.info(
            "Committed batch %d of %d",
            idx // BATCH_SIZE + 1,
            ((len(all_files) - 1) // BATCH_SIZE) + 1,
        )

    # 6) Tear down
    session.close()
    logger.info("All done.")


if __name__ == "__main__":
    run_sql_pipeline()
