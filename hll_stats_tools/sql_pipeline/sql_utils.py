import functools
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from itertools import chain, pairwise
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from hll_stats_tools.sql_pipeline.models import (
    Game,
    GameAnalysis,
    Player,
    PlayerAnalysis,
)
from hll_stats_tools.utils.logger_utils import setup_logger

logger = setup_logger(__name__)

# from hll_stats_tools.sql_tools.models import Game, Event, Player


def batch_operation(
    model,  # SQLAlchemy ORM class, e.g. Game or Event
    db_url=None,  # if None, will read from .env
    batch_size=500,
):
    """
    Decorator factory: loops over every row of `model` in the DB,
    calling the decorated function once per instance, inside a managed session.
    """

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            # 1. Resolve DB URL
            load_dotenv(".env")
            url = db_url or os.getenv("sql_database")
            if not url:
                raise RuntimeError("sql_database not set in .env or decorator arg")

            # 2. Create engine & session
            engine = create_engine(url, echo=False)
            Session = sessionmaker(bind=engine, autoflush=False)
            session = Session()

            try:
                # 3. Count & announce
                total = session.query(
                    func.count(model.__table__.c[model.__mapper__.primary_key[0].name])
                ).scalar()
                logger.info(
                    "Found %d %s rows; processing in batches of %d…",
                    total,
                    model.__tablename__,
                    batch_size,
                )

                # 4. Stream through rows
                query = (
                    session.query(model)
                    .order_by(*model.__mapper__.primary_key)
                    .yield_per(batch_size)
                    .enable_eagerloads(False)
                )

                for idx, instance in enumerate(query, start=1):
                    # call your function with (session, instance, *args, **kwargs)
                    fn(session, instance, *args, **kwargs)

                    # commit every batch_size to flush to disk & free memory
                    if idx % batch_size == 0:
                        session.commit()
                        session.expire_all()
                        logger.info("  …processed %d/%d", idx, total)

                # final commit
                session.commit()
                logger.info("Batch operation complete.")
            finally:
                session.close()

        return wrapper

    return decorator


def distributions(game, player_id, total_time):
    kill_events = [
        ev
        for ev in game.events
        if ev.type == "KILL"
        and (ev.player1_id == player_id or ev.player2_id == player_id)
    ]
    kill_distribution = defaultdict(list)
    death_distribution = defaultdict(list)
    weapons_kill_distribution = defaultdict(list)
    weapons_death_distribution = defaultdict(list)
    tot_kills = 0
    tot_deaths = 0

    for ev in kill_events:
        if ev.player1_id == player_id:
            offset = (ev.event_time - game.start_time).total_seconds()
            kill_distribution[offset].append(ev.player2_id)
            weapons_kill_distribution[offset].append(ev.weapon)
            tot_kills += 1
        else:
            offset = (ev.event_time - game.start_time).total_seconds()
            death_distribution[offset].append(ev.player1_id)
            weapons_death_distribution[offset].append(ev.weapon)
            tot_deaths += 1

    team_kill_distribution = defaultdict(list)  # Counter{}
    team_death_distribution = defaultdict(list)
    tot_team_kills = 0
    tot_team_deaths = 0
    team_kill_events = [
        ev
        for ev in game.events
        if ev.type == "TEAM KILL"
        and (ev.player1_id == player_id or ev.player2_id == player_id)
    ]
    for ev in team_kill_events:
        if ev.player1_id == player_id:
            team_kill_distribution[
                (ev.event_time - game.start_time).total_seconds()
            ].append(ev.player2_id)
            tot_team_kills += 1
        else:
            team_death_distribution[
                (ev.event_time - game.start_time).total_seconds()
            ].append(ev.player1_id)
            tot_team_deaths += 1
    if total_time == 0:
        kpm = 0
        dpm = 0
    else:
        kpm = tot_kills / (total_time / 60)
        dpm = tot_deaths / (total_time / 60)

    return (
        kill_distribution,
        death_distribution,
        team_kill_distribution,
        team_death_distribution,
        weapons_kill_distribution,
        weapons_death_distribution,
        tot_kills,
        tot_deaths,
        tot_team_kills,
        tot_team_deaths,
        kpm,
        dpm,
    )


def calc_player_stats(game, player_id):
    actual_start_time = game.start_time + timedelta(minutes=5)
    timelimits = [
        ev
        for ev in game.events
        if ev.type in ("CONNECTED", "DISCONNECTED") and ev.player1_id == player_id
    ]
    if len(timelimits) == 0:
        total_time = game.duration - 300
    else:

        times = sorted(
            ((event.event_time, event.type) for event in timelimits), key=lambda x: x[0]
        )
        if times[0][1] == "DISCONNECTED":
            new_start = (actual_start_time, "CONNECTED")
            times.insert(0, new_start)
        if times[-1][1] == "CONNECTED":
            new_end = (game.end_time, "DISCONNECTED")
            times.append(new_end)
        if len(times) % 2 != 0:
            entries = ",".join([str(x[1]) for x in times])
            logger.warning(
                "Times len is odd in game %s, player %s:\n%s\n%s",
                game.game_key,
                player_id,
                times,
                entries,
            )
            return None
        total_time = 0
        for start, end in pairwise(times):
            total_time += (end[0] - start[0]).total_seconds()

    (
        kill_distribution,
        death_distribution,
        team_kill_distribution,
        team_death_distribution,
        weapons_kill_distribution,
        weapons_death_distribution,
        tot_kills,
        tot_deaths,
        tot_team_kills,
        tot_team_deaths,
        kpm,
        dpm,
    ) = distributions(game, player_id, total_time)

    nemesis = dict(Counter(chain.from_iterable(death_distribution.values())))
    victims = dict(Counter(chain.from_iterable(kill_distribution.values())))
    weapons_kills = dict(
        Counter(chain.from_iterable(weapons_kill_distribution.values()))
    )
    weapons_deaths = dict(
        Counter(chain.from_iterable(weapons_death_distribution.values()))
    )

    return {
        "player_id": player_id,
        "time_played_seconds": total_time,
        "kpm": kpm,
        "dpm": dpm,
        "tot_kills": tot_kills,
        "tot_deaths": tot_deaths,
        "ratio": tot_kills / tot_deaths if tot_deaths != 0 else tot_kills,
        "tot_team_kills": tot_team_kills,
        "tot_team_deaths": tot_team_deaths,
        "kill_distribution": kill_distribution,
        "death_distribution": death_distribution,
        "team_kill_distribution": team_kill_distribution,
        "team_death_distribution": team_death_distribution,
        "nemesis": nemesis,
        "victims": victims,
        "weapons_kills": weapons_kills,
        "weapons_deaths": weapons_deaths,
    }


###### Useful Queries for plotting


def get_games_player(
    session, player_id, server=1, date_start: datetime = None, date_end: datetime = None
):

    filters = [Game.server == server]
    if date_start:
        filters.append(Game.start_time >= date_start)
    if date_end:
        filters.append(Game.start_time <= date_end)
    if player_id:
        filters.append(Game.players.any(Player.player_id == player_id))
    games = session.query(Game).filter(*filters).all()

    return games


def grab_game_by_id(session, game):
    filters = [Game.game_key == game]
    games = session.query(Game).filter(*filters).all()
    return games


def grab_game_by_start(session, start):
    filters = [Game.start_time == start]
    games = session.query(Game).filter(*filters).all()
    return games


def grab_player_plot(
    session,
    player_id: str,
    date_start: datetime,
    date_end: datetime,
    metric_column,
    round_to=2,
):
    results = (
        session.query(func.date(Game.start_time), func.avg(metric_column))
        .join(GameAnalysis, PlayerAnalysis.analysis_id == GameAnalysis.id)
        .join(Game, GameAnalysis.game_key == Game.game_key)
        .filter(
            PlayerAnalysis.player_id == player_id,
            Game.start_time >= date_start,
            Game.start_time <= date_end,
        )
        .group_by(func.date(Game.start_time))
        .order_by(func.date(Game.start_time))
        .all()
    )

    metric_by_date = {str(date): round(metric, round_to) for date, metric in results}
    return metric_by_date
