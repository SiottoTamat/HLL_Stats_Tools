# hll_stats_tools/pipeline.py

import yaml
from pathlib import Path
import json, os
from dotenv import load_dotenv
from datetime import date

from .talk_to_server import download_sequential_logs
from .logs_utils import merge_logs_to_games
from .analysis_utils import refill_analysis_folder
from .statistics import create_plots, player_plots_from_fileplot, pandarize_plots
from .make_plot import plot_player_data

from .runner import (
    run_analysis,
    run_plots,
    run_make_player_plot,
    run_extract_player_plot,
    run_split_game_logs,
    run_fetch_logs,
)


def run_pipeline():
    # load config.yaml
    cfg = yaml.safe_load(Path("config.yaml").read_text())

    # pull flags
    update_logs = cfg["update_logs"]
    split_logs_to_games = cfg["split_logs_to_games"]
    create_analysis = cfg["create_analysis"]
    create_monthly_plots = cfg["create_monthly_plots"]
    extract_player_plot = cfg["extract_player_plot"]
    make_stats = cfg["make_stats"]
    metrics = cfg["metrics"]
    focus_player = cfg["focus_player"]

    # load env
    load_dotenv(".env")
    out_historical = Path(os.getenv("out_folder_historical_logs"))
    out_game = Path(os.getenv("out_folder_game_logs"))
    out_analysis = Path(os.getenv("out_folder_analysis"))
    out_plots = Path(os.getenv("out_folder_plots"))
    out_player_plots = Path(os.getenv("out_folder_player_plots"))

    # load group
    group_members_json = os.getenv("group_members_json")
    GROUP = json.loads(Path(group_members_json).read_text())
    group_filter = {x: v[0] for x, v in GROUP.items()}

    # orchestrate
    if update_logs:
        run_fetch_logs(out_historical, cfg["update_to_last_minute"])

    if split_logs_to_games:
        run_split_game_logs(
            out_historical, out_game  # , overwrite=cfg["overwrite_game_logs"]
        )

    if create_analysis:
        run_analysis(out_analysis, out_game)

    if create_monthly_plots:
        run_plots(
            out_analysis, out_plots, group_filter, filter_name=os.getenv("group_name")
        )

    if extract_player_plot:
        run_extract_player_plot(
            focus_player,
            metrics,
            out_plots,
            out_player_plots,
            start_date=date(2024, 1, 1),
        )

    if make_stats:
        if focus_player:
            pdata = json.loads(
                (out_player_plots / f"{focus_player}_stats.json").read_text()
            )
            run_make_player_plot(
                focus_player,
                pdata,
                metrics,
                group_filter,
                namefile=out_player_plots / f"{focus_player}_plot.png",
            )
