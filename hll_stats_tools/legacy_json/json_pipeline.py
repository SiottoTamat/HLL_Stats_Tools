import json
import os
from datetime import date
from pathlib import Path

import yaml
from dotenv import load_dotenv

from hll_stats_tools.utils.logger_utils import setup_logger

from .runner import (
    run_analysis,
    run_extract_player_plot,
    run_fetch_logs,
    run_make_player_plot,
    run_plots,
    run_split_game_logs,
)

logger = setup_logger(__name__)


def run_json_pipeline():
    # load config.yaml
    cfg = yaml.safe_load(Path("config.yaml").read_text())

    # pull flags
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

    if split_logs_to_games:
        logger.info("splitting logs to games")
        run_split_game_logs(
            out_historical, out_game  # , overwrite=cfg["overwrite_game_logs"]
        )

    if create_analysis:
        logger.info("creating analysis")
        run_analysis(out_analysis, out_game)

    if create_monthly_plots:
        logger.info("creating plots")
        run_plots(
            out_analysis, out_plots, group_filter, filter_name=os.getenv("group_name")
        )

    if extract_player_plot:
        logger.info("extracting player plot")
        run_extract_player_plot(
            focus_player,
            metrics,
            out_plots,
            out_player_plots,
            start_date=date(2024, 1, 1),
        )

    if make_stats:
        logger.info("making stats")
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
