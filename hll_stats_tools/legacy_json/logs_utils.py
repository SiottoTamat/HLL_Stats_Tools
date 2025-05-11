import json
from datetime import datetime
from pathlib import Path

from hll_stats_tools.utils.common_utils import recuperate_date
from hll_stats_tools.utils.common_utils import openfile


def merge_logs_to_games(
    folder: str | Path,
    output_folder: str | Path,
    verbose=False,
    server: str = "1",
    overwrite: bool = False,
) -> None:
    folder = Path(folder)
    output_folder = Path(output_folder)
    current_game = {"date": "before_time", "map": "None", "logs": []}
    current_game_ids = set()
    if not overwrite:
        log_start = get_end(openfile(sorted(output_folder.glob("*.json"))[-1]))
        start_from = log_start[:10]

        # "2025-03-02T21:48:46" -> example
        files = get_files_after_date(folder, start_from)
    else:
        files = folder.glob("*.json")
    for in_file in sorted(files):  # (folder.glob("*.json")):
        if verbose:
            print(in_file.name)
        data = openfile(in_file)
        # first_match = False
        for log in data:
            if log["event_time"] >= log_start:
                if log["server"] == server:
                    if log["id"] not in current_game_ids:
                        if log["type"] == "MATCH START":
                            clean_date = current_game["date"].replace(":", "-")
                            name_game = f"{clean_date}_{current_game['map']}.json"
                            file = output_folder / Path(name_game)
                            with file.open("w", encoding="utf-8") as f:
                                json.dump(current_game, f, indent=4)
                            current_game_ids.clear()
                            current_game_ids.add(log["id"])
                            current_game = {"date": "", "map": "", "logs": []}
                            current_game["date"] = log["event_time"]
                            current_game["map"] = extract_map(log)

                            current_game["logs"].append(log)
                        else:
                            current_game["logs"].append(log)
                            current_game_ids.add(log["id"])


def extract_map(log: dict) -> str:
    if log["type"] == "MATCH START":
        return log["content"].replace("MATCH START ", "").strip()
    if log["type"] == "MATCH ENDED":
        return log["content"].split("`")[1].strip()


def get_end(match: dict) -> str:
    for log in match["logs"]:
        if log["type"] == "MATCH ENDED":
            return log["event_time"]


def get_files_after_date(folder: str | Path, date: str) -> list:
    def get_date(file: Path) -> datetime:
        _datetime = datetime.fromisoformat(recuperate_date(file.stem))
        return _datetime.date()

    folder = Path(folder)
    return [
        file
        for file in folder.glob("*.json")
        if get_date(file) >= datetime.fromisoformat(date).date()
    ]  # [file for file in folder.glob("*.json") if get_date(file) > date]


def check_game(data: dict) -> list:

    starts = [x for x in data["logs"] if x["type"] == "MATCH START"]
    ends = [x for x in data["logs"] if x["type"] == "MATCH ENDED"]

    if len(starts) > 1 or len(ends) > 1:
        return {"starts": starts, "ends": ends, "map_problem": True}
    if len(starts) == 0 or len(ends) == 0:
        return {"starts": starts, "ends": ends, "map_problem": True}
    else:
        if extract_map(starts[0]) != extract_map(ends[0]):
            return {"starts": starts, "ends": ends, "map_problem": True}
    return None


def main():
    pass


if __name__ == "__main__":
    main()
