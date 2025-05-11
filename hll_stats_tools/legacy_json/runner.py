import json
from datetime import date
from pathlib import Path

from hll_stats_tools.legacy_json.analysis_utils import refill_analysis_folder
from hll_stats_tools.legacy_json.logs_utils import merge_logs_to_games
from hll_stats_tools.legacy_json.statistics import (
    create_plots,
    pandarize_plots,
    player_plots_from_fileplot,
)
from hll_stats_tools.utils.logger_utils import setup_logger

from ..plotting.make_plot import plot_player_data

logger = setup_logger(__name__)


def run_split_game_logs(
    out_folder_historical_logs, out_folder_game_logs, overwrite: bool = False
):

    merge_logs_to_games(
        out_folder_historical_logs,
        out_folder_game_logs,
        overwrite=overwrite,
    )
    logger.info("Done with Split logs to games")


def run_analysis(out_folder_analysis, out_folder_game_logs):
    refill_analysis_folder(out_folder_analysis, out_folder_game_logs)
    logger.info("Created analysis")


def run_plots(out_folder_analysis, out_folder_plots, group_filter, filter_name=None):
    create_plots(
        out_folder_analysis, out_folder_plots, group_filter, filter_name=filter_name
    )
    logger.info("Created plots")


def run_extract_player_plot(
    this_player,
    metrics,
    out_folder_plots,
    out_folder_player_plots,
    start_date: date | None = None,
    end_date: date | None = None,
):

    player = player_plots_from_fileplot(
        out_folder_plots,
        this_player,
        plots=metrics,
        start_date=start_date,
        end_date=end_date,
    )
    newfile = Path(out_folder_player_plots) / f"{this_player}_stats.json"
    with newfile.open("w", encoding="utf-8") as f:
        json.dump(player, f, indent=4)
    logger.info("Extracted player %s plot", this_player)


def run_make_player_plot(
    player_id,
    player_data,
    metrics,
    group_filter: dict = None,
    namefile: Path | None = None,
    constant_multiplier: float = 2.5,
    timeframe_group_by="week",
):

    if namefile:
        namefile = Path(namefile)

    df = pandarize_plots(player_id, metrics, player_data)
    if not df.empty:
        plot_player_data(
            df,
            timeframe_group_by=timeframe_group_by,
            group_names=group_filter,
            constant_multiplier=constant_multiplier,
            namefile=namefile,
        )
    logger.info("Created player %s plot", player_id)
