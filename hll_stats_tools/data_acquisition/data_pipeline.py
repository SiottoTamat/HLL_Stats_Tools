import os
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from dotenv import load_dotenv

from hll_stats_tools.data_acquisition.talk_to_server import download_sequential_logs
from hll_stats_tools.utils.common_utils import openfile
from hll_stats_tools.utils.logger_utils import setup_logger

cfg = yaml.safe_load(Path("config.yaml").read_text())

update_to_last_minute = cfg["update_to_last_minute"]
load_dotenv(".env")
historical_logs_folder = Path(os.getenv("out_folder_historical_logs"))


logger = setup_logger(__name__)


def run_data_pipeline(
    out_folder_historical_logs=historical_logs_folder,
    update_to_last_minute=update_to_last_minute,
):
    logger.info("Running data pipeline")
    last_log_file = sorted(out_folder_historical_logs.glob("*.json"))[-1]
    last_log_time = openfile(last_log_file)[-1][
        "event_time"
    ]  # example:"2025-04-08T17:16:52"
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    download_sequential_logs(out_folder_historical_logs, last_log_time, yesterday)
    if update_to_last_minute:
        download_sequential_logs(
            out_folder_historical_logs,
            last_log_time,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        logger.info("Updated logs to last minute")
    logger.info("Updated logs")
