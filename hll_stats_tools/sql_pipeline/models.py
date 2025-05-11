import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    or_,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

game_players = Table(
    "game_players",
    Base.metadata,
    Column("game_key", String, ForeignKey("games.game_key"), primary_key=True),
    Column("player_id", String, ForeignKey("players.player_id"), primary_key=True),
)


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
    player1_id = Column(String, ForeignKey("players.player_id"), index=True)
    player2_id = Column(String, ForeignKey("players.player_id"), index=True)
    player2_name = Column(String)

    weapon = Column(String)

    # The raw/textual data and any content string
    raw = Column(Text)
    content = Column(Text)

    # Where this came from
    server = Column(String)

    game_key = Column(String, ForeignKey("games.game_key"), nullable=True, index=True)
    game = relationship("Game", back_populates="events")
    player1 = relationship(
        "Player", back_populates="events_as_p1", foreign_keys=[player1_id]
    )
    player2 = relationship(
        "Player", back_populates="events_as_p2", foreign_keys=[player2_id]
    )

    # When we inserted into SQLite
    inserted_at = Column(DateTime, default=datetime.now, nullable=False)

    __table_args__ = (
        # single-column indexes
        Index("idx_events_type", "type"),
        Index("idx_events_event_time", "event_time"),
        Index("idx_events_creation_time", "creation_time"),
        Index("idx_events_inserted_at", "inserted_at"),
        # Index("idx_events_player1_id", "player1_id"),
        # Index("idx_events_player2_id", "player2_id"),
        Index("idx_events_server", "server"),
        Index("idx_events_weapon", "weapon"),
        # composite indexes
        Index("idx_events_player1_time", "player1_id", "event_time"),
        Index("idx_events_player2_time", "player2_id", "event_time"),
        Index("idx_events_type_time", "type", "event_time"),
        Index("idx_events_type_player1_time", "type", "player1_id", "event_time"),
        Index("idx_events_type_player2_time", "type", "player2_id", "event_time"),
    )


class Game(Base):
    __tablename__ = "games"

    # Composite ID as a single string: e.g. "server3_32"
    game_key = Column(String, primary_key=True)

    # Breakdown of the key for querying if you ever need it:
    server = Column(String, nullable=False, index=True)
    game_number = Column(Integer, nullable=False, index=True)
    seeding = Column(Boolean, nullable=False, default=False, index=True)

    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, nullable=True, index=True)
    ended = Column(Boolean, default=False, nullable=False, index=True)

    map = Column(String, index=True)
    mode = Column(String, index=True)
    duration = Column(Integer, nullable=True, index=True)
    allied_score = Column(Integer, nullable=True, index=True)
    axis_score = Column(Integer, nullable=True, index=True)
    winner = Column(String, nullable=True, index=True)  # "allied" or "axis"

    fix_applied = Column(DateTime, nullable=True, index=True)
    fix_description = Column(String, nullable=True)

    events = relationship("Event", back_populates="game")
    players = relationship(
        "Player",
        secondary=game_players,
        back_populates="games",
        viewonly=True,  # since we'll manage this via ingestion logic
    )
    analyses = relationship(
        "GameAnalysis", back_populates="game", cascade="all, delete-orphan"
    )

    __table_args__ = (
        # single-column indexes
        # Index("idx_games_server", "server"),
        # Index("idx_games_game_number", "game_number"),
        # Index("idx_games_start_time", "start_time"),
        # Index("idx_games_end_time", "end_time"),
        # Index("idx_games_ended", "ended"),
        # Index("idx_games_map", "map"),
        # Index("idx_games_mode", "mode"),
        # Index("idx_games_duration", "duration"),
        # Index("idx_games_allied_score", "allied_score"),
        # Index("idx_games_axis_score", "axis_score"),
        # Index("idx_games_winner", "winner"),
        # Index("idx_games_seeding", "seeding"),
        # composite indexes
        Index("idx_games_map_mode", "map", "mode"),
        Index("idx_games_server_start", "server", "start_time"),
    )


class Player(Base):
    __tablename__ = "players"

    player_id = Column(String, primary_key=True, nullable=False)
    current_name = Column(String, nullable=False)
    first_seen = Column(DateTime, default=datetime.now, nullable=False)
    last_seen = Column(
        DateTime, default=datetime.now, onupdate=datetime.now, nullable=False
    )
    fix_applied = Column(DateTime, nullable=True, index=True)
    fix_description = Column(String, nullable=True)

    name_history = relationship(
        "PlayerName",
        back_populates="player",
        order_by="PlayerName.changed_at",
        cascade="all, delete-orphan",
    )
    events_as_p1 = relationship(
        "Event",
        back_populates="player1",
        foreign_keys="[Event.player1_id]",
    )
    events_as_p2 = relationship(
        "Event",
        back_populates="player2",
        foreign_keys="[Event.player2_id]",
    )

    games = relationship(
        "Game",
        secondary=game_players,
        back_populates="players",
        viewonly=True,
    )

    __table_args__ = (
        Index("ix_players_current_name", "current_name"),
        Index("ix_players_first_seen", "first_seen"),
    )


class PlayerName(Base):
    __tablename__ = "player_names"

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(String, ForeignKey("players.player_id"), nullable=False)
    name = Column(String, nullable=False, index=True)
    changed_at = Column(DateTime, default=datetime.now, nullable=False)

    player = relationship("Player", back_populates="name_history")

    __table_args__ = (
        # you might query “what players used X?” so index on name
        Index("ix_player_names_player_changed", "player_id", "changed_at"),
    )
    fix_applied = Column(DateTime, nullable=True, index=True)
    fix_description = Column(String, nullable=True)


class ProcessedFile(Base):
    __tablename__ = "processed_files"

    filename = Column(String, primary_key=True)
    ingested_at = Column(DateTime, default=datetime.now, nullable=False)


class GameAnalysis(Base):
    __tablename__ = "game_analyses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_key = Column(String, ForeignKey("games.game_key"), nullable=False, index=True)
    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    game = relationship("Game", back_populates="analyses")
    player_stats = relationship(
        "PlayerAnalysis", back_populates="analysis", cascade="all, delete-orphan"
    )
    fix_applied = Column(DateTime, nullable=True, index=True)
    fix_description = Column(String, nullable=True)


class PlayerAnalysis(Base):
    __tablename__ = "player_analyses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    analysis_id = Column(
        Integer, ForeignKey("game_analyses.id"), nullable=False, index=True
    )
    player_id = Column(
        String, ForeignKey("players.player_id"), nullable=False, index=True
    )

    # summary metrics
    tot_kills = Column(Integer, nullable=False)
    tot_deaths = Column(Integer, nullable=False)
    tot_team_kills = Column(Integer, nullable=False)
    tot_team_deaths = Column(Integer, nullable=False)
    kpm = Column(Float, nullable=False)
    dpm = Column(Float, nullable=False)
    ratio = Column(Float, nullable=False)
    time_played_secs = Column(Float, nullable=False)

    # distributions as JSON blobs
    kill_distribution = Column(Text)  # store json.dumps(kill_distribution)
    death_distribution = Column(Text)
    team_kill_distribution = Column(Text)
    team_death_distribution = Column(Text)
    weapons_kill_distribution = Column(Text)
    weapons_death_distribution = Column(Text)

    analysis = relationship("GameAnalysis", back_populates="player_stats")
    player = relationship("Player")

    fix_applied = Column(DateTime, nullable=True, index=True)
    fix_description = Column(String, nullable=True)
