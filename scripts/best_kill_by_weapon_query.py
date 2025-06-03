import json
import os
from collections import namedtuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from hll_stats_tools.sql_pipeline.models import PlayerAnalysis

load_dotenv(".env")
sql_database = os.getenv("sql_database")

data = json.load(open("event_weapons.json"))

# weapon_groups = {
#     "mines": [
#         "A.P. Shrapnel Mine Mk II",
#         "A.T. Mine G.S. Mk V",
#         "M1A1 AT MINE",
#         "M2 AP MINE",
#         "POMZ AP MINE",
#         "S-MINE",
#         "SMine",
#         "TELLERMINE 43",
#         "TM-35 AT MINE",
#         "Tellermine43",
#     ],
#     "melee weapons": [
#         "M3 KNIFE",
#         "Knife_US",
#         "FELDSPATEN",
#         "Spade_GER",
#         "MPL-50 SPADE",
#     ],
#     "satchel": [
#         "Satchel",
#         "Satchel_3KG",
#         "Satchel_M37",
#         "SATCHEL",
#         "SATCHEL CHARGE",
#     ],
#     "semiauto": ["GEWEHR 43", "Garand", "M1 GARAND", "SVT40"],
# }

weapons_of_interest = [
    key
    for key, values in data.items()
    if values["common_name"] == "grease_gun"
]
# weapons_of_interest = [
#     key for key, values in data.items() if values["group"] == "automatic"
# ]
weapon_group_name = "grease_gun"

# weapons_of_interest = weapon_groups[weapon_group_name]
# weapons_of_interest = ["BOMBING RUN"]

# --- Set up the database connection ---
engine = create_engine(sql_database, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

# ————— Data Structures —————
KillEntry = namedtuple("KillEntry", ["player", "game", "count"])
entries: list[KillEntry] = []

# ————— Collect Counts —————
for pa in session.scalars(select(PlayerAnalysis)).all():
    raw = pa.weapons_kill_distribution
    if not raw:
        continue
    try:
        dist = json.loads(raw)
    except json.JSONDecodeError:
        continue
    # Sum only the weapons you care about:
    count = sum(dist.get(w, 0) for w in weapons_of_interest)
    if count == 0:
        continue

    # Navigate relationships
    player = pa.player  # Player instance
    ga = pa.analysis  # GameAnalysis instance
    if not ga or not ga.game:
        continue
    game = ga.game  # Game instance

    entries.append(KillEntry(player=player, game=game, count=count))

# ————— Sort & Show Top 10 —————
top_10 = sorted(entries, key=lambda e: e.count, reverse=True)[:10]

for rank, entry in enumerate(top_10, start=1):
    p = entry.player
    g = entry.game
    print(
        f"{rank}. {p.current_name} — {entry.count} kills with “{weapon_group_name}” "
        f"on map {g.map} at {g.start_time}"
    )

session.close()
