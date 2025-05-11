import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from hll_stats_tools.plotting.make_plot import plot_multiple_metrics
from hll_stats_tools.sql_pipeline.models import (
    PlayerAnalysis,
)
from hll_stats_tools.sql_pipeline.sql_utils import grab_player_plot

load_dotenv(".env")
sql_database = os.getenv("sql_database")
ESPTjson = os.getenv("group_members_json")
ESPT = json.load(open(ESPTjson))
out_png_folder = os.getenv("group_png_folder")


def main():
    db_url = os.getenv("sql_database")
    if not db_url:
        raise RuntimeError("sql_database not set in .env")
    engine = create_engine(db_url, echo=False)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        for id, names in ESPT.items():
            name = names[0]
            today = datetime.today().strftime("%Y-%m-%d")
            file_out_name = f"{today}_{name}_kpm_dpm.png"
            file_out_name = Path(out_png_folder) / file_out_name

            start = datetime(2024, 1, 1)
            end = datetime.now()
            title = f"{name} KPM & DPM"
            print(f">>>plotting {name} in {file_out_name}")

            kpm = grab_player_plot(session, id, start, end, PlayerAnalysis.kpm)
            dpm = grab_player_plot(session, id, start, end, PlayerAnalysis.dpm)
            plot_multiple_metrics(
                {"KPM": kpm, "DPM": dpm},
                title=title,
                group_by="W",
                namefile=file_out_name,
                rolling_average=7,
                display_rolling_average_overlay=True,
            )


if __name__ == "__main__":
    main()
