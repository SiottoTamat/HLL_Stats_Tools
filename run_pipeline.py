from pathlib import Path

import yaml

from hll_stats_tools.data_acquisition.data_pipeline import run_data_pipeline
from hll_stats_tools.legacy_json.json_pipeline import run_json_pipeline
from hll_stats_tools.sql_pipeline.ingest_events import run_sql_pipeline

cfg = yaml.safe_load(Path("config.yaml").read_text())


data_acquisition = cfg["run_data_pipeline"]
json_pipeline = cfg["run_json_pipeline"]
sql_pipeline = cfg["run_sql_pipeline"]


if __name__ == "__main__":
    if data_acquisition:
        run_data_pipeline()

    if json_pipeline:
        run_json_pipeline()

    if sql_pipeline:
        run_sql_pipeline()
