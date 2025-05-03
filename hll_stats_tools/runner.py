from datetime import datetime, timedelta, date
import json
import os
from pathlib import Path

from .talk_to_server import download_sequential_logs
from .logs_utils import merge_logs_to_games
from .analysis_utils import refill_analysis_folder
from .statistics import (
    create_plots,
    player_plots_from_fileplot,
    pandarize_plots,
)
from .make_plot import plot_player_data
from .utils import openfile


def run_fetch_logs(out_folder_historical_logs, update_to_last_minute):

    last_log_file = sorted(out_folder_historical_logs.glob("*.json"))[-1]
    last_log_time = openfile(last_log_file)[-1]["event_time"]  # "2025-04-08T17:16:52"
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    download_sequential_logs(out_folder_historical_logs, last_log_time, yesterday)
    if update_to_last_minute:
        download_sequential_logs(
            out_folder_historical_logs,
            last_log_time,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        print("Updated logs to last minute")
    print("Updated logs")  # updated_logs


def run_split_game_logs(
    out_folder_historical_logs, out_folder_game_logs, overwrite: bool = False
):

    merge_logs_to_games(
        out_folder_historical_logs,
        out_folder_game_logs,
        overwrite=overwrite,
    )
    print("Split logs to games")


def run_analysis(out_folder_analysis, out_folder_game_logs):
    refill_analysis_folder(out_folder_analysis, out_folder_game_logs)
    print("Created analysis")


def run_plots(out_folder_analysis, out_folder_plots, group_filter, filter_name=None):
    create_plots(
        out_folder_analysis, out_folder_plots, group_filter, filter_name=filter_name
    )
    print("Created plots")


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
    print(f"extracted player {this_player} plot")


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
    print("created player plot")
