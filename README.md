# HLL Stats Tools

**HLL Stats Tools** is a modular toolkit for downloading, storing, analyzing, and visualizing player performance data from _Hell Let Loose_ server logs. It supports both a legacy JSON-based processing mode and a modern SQL-backed pipeline using SQLAlchemy and SQLite.

---

## 📦 Features

- **Dual Pipelines**: Supports both JSON- and SQL-based analysis workflows
- **Structured Data Ingestion**: Pulls logs from the HLL API, deduplicates, and stores
- **Relational Modeling**: SQL schema for players, games, events, and analyses
- **Metrics and Visualizations**: Calculates and plots KPM, DPM, and ratios over time
- **Extensible Architecture**: Modular code split into logical packages
- **CI Integration**: Automated testing pipeline with GitHub Actions

---

## 🗂️ Project Structure

```
hll_stats_tools/
├── data_acquisition/      # API data collection logic
├── legacy_json/           # JSON processing pipeline (legacy)
├── sql_pipeline/          # SQL-based ingestion and analysis
├── plotting/              # Visualization tools
├── utils/                 # Shared utilities (logging, time handling, etc.)
.github/workflows/ci.yaml  # Continuous Integration pipeline
run_pipeline.py            # Unified entrypoint for both pipelines
config.yaml                # Configuration file
.env                       # Environment variables (e.g., API keys)
```

---

## ⚙️ Configuration

Edit `config.yaml` to control input/output paths, database file, and behavior toggles.

```yaml
data_acquisition:
  output_folder: "data/logs"
  update_to_last_minute: true

sql_pipeline:
  db_path: "data/hll_stats.db"
  force_reset: false

json_pipeline:
  json_folder: "data/json_logs"
  output_folder: "data/json_analyses"
```
you will have to setup your own .env file with these parameters:
- API_KEY="apikey000#"
- log_file="path" - where your log will reside
- out_folder_historical_logs="path" - where the downloaded logs will reside
- out_folder_game_logs="path" - (if you use the json version) where your split games will be
- out_folder_analysis="path" - (if you use the json version) where your game analysis files will be
- out_folder_plots="path" - (if you use the json version) where your player plots will be
- out_folder_player_plots="path" - (if you use the json version) where your png plots will be
- group_name='ESPT' - this is a name of a group (if you want to create a group of players you want to analyse seamlessly)
- group_members_json="path" - a json file where you keep all steam-id# (key) and in-game-names (value) of the players of the group
- sql_database="sqlite:path/hll_stats.db" where you want to save your database (~12Gb per year of server operations)
- force_reset="False"
- group_png_folder="-path" where to store your group png analysis (optional - see plot_all_ESPT.py example in scripts/)

Sensitive or runtime overrides (like `FORCE_RESET`) are pulled from `.env`:

```env
FORCE_RESET=true
```

---

## 🚀 Usage

### Unified Entrypoint

```bash
python run_pipeline.py
```

Behavior is controlled by `config.yaml`. The script will:
- Download and update logs
- Process them into the SQLite DB or JSON folders
- Optionally run fixes (e.g. KPM/DPM bug correction)
- Generate plots if configured

---

## 📊 Plotting

```python
from hll_stats_tools.plotting.make_plot import plot_multiple_metrics

plot_multiple_metrics(
    metrics_by_date={
        "KPM": kpm_data,
        "DPM": dpm_data
    },
    group_by="W",  # 'D', 'W', 'M'
    rolling_average=3,
    display_rolling_average_overlay=True,
    title="Weekly Performance",
    namefile=None
)
```

---

## 🧪 Development

- Use `logger_utils.setup_logger(__name__)` for consistent project-wide logging
- All logging output goes to `logs/hll_stats.log` (and console)
- Format checked with `flake8`, metadata in `pyproject.toml`

---

## 🧼 Linting and CI

```bash
flake8 hll_stats_tools
```

GitHub Actions will run basic checks on every push using `.github/workflows/ci.yaml`.

---

## 📄 License

MIT License. Attribution appreciated but not required.

---

## 🙌 Credits

Developed by [Andrea Siotto](https://github.com/SiottoTamat).
Big thanks to the ESPT community!
