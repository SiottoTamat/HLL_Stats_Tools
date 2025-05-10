import json
from datetime import date, datetime
from pathlib import Path


def openfile(file: str | Path) -> dict:
    if isinstance(file, str):
        file = Path(file)
    with file.open("r", encoding="utf-8") as f:
        try:
            data = json.loads(f.read())
        except:
            data = None
    return data


def grab_games_by_dates(
    folder: Path | str, year, month="", day="", separator=""
) -> list:
    """
    Grabs a list of files from a given folder filtered by year, month and day.

    Parameters:
    folder (Path | str): Path to the folder containing the files.
    year (str): The year to filter by.
    month (str, optional): The month to filter by. Defaults to "".
    day (str, optional): The day to filter by. Defaults to "".

    Returns:
    list: A list of files that match the given filter.
    """
    year = f"{year}"
    month = f"{month:02}" if month else ""
    day = f"{day:02}" if day else ""
    parameters = [year, month, day]
    filter = separator.join(s for s in parameters if s != "")
    # f"{[year,month,day]}{year}{separator}{month}{separator}{day}"
    folder = Path(folder)
    returned = [file for file in folder.glob(f"{filter}*.json")]
    return returned


def recuperate_date(date: str) -> datetime:
    """
    Recuperate a date string that was wrongly formatted.

    The date string was originally in the format "YYYY-MM-DDTHH-MM-SSZ"
    but it was wrongly formatted as "YYYY-MM-DDTHH-MM-SS-HH-MMZ"

    Parameters
    ----------
    date : str
        The wrongly formatted date string

    Returns
    -------
    datetime
        The correctly formatted date string
    """
    # Split the string into two parts
    splitted = date.split("T")
    # Replace the last four characters with a colon
    return f"{splitted[0]}T{splitted[1].replace('-', ':')}"


def recuperate_datetime(date: str) -> datetime:
    """
    Recuperate a date string that was wrongly formatted.

    The date string was originally in the format "YYYY-MM-DDTHH-MM-SSZ"
    but it was wrongly formatted as "YYYY-MM-DDTHH-MM-SS-HH-MMZ"

    Parameters
    ----------
    date : str
        The wrongly formatted date string

    Returns
    -------
    datetime
        The correctly formatted date string
    """
    # Split the string into two parts
    if "T" not in date:
        return None
    splitted = date.split("T")
    # Replace the last four characters with a colon
    isostring = f"{splitted[0]}T{splitted[1].replace('-', ':')}"
    try:
        return datetime.fromisoformat(isostring)
    except:
        return None


def start_end_isostring(game: dict) -> list:
    return sorted(
        [
            x["event_time"]
            for x in game["logs"]
            if x["type"] == "MATCH START" or x["type"] == "MATCH ENDED"
        ]
    )


def only_actual_game_logs(game: dict) -> dict:
    start_idx = -1
    end_idx = -1
    for index, log in enumerate(game["logs"]):
        if log["type"] == "MATCH START":
            start_idx = index
        if log["type"] == "MATCH ENDED":
            end_idx = index
    game["logs"] = game["logs"][start_idx : end_idx + 1]
    return game


def month_year_iter(start_date: date, end_date: date):
    current = date(start_date.year, start_date.month, 1)
    while current <= end_date:
        yield current.year, current.month
        # move to the first day of the next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


def deep_merge(d1, d2):
    """Recursively merge two dictionaries."""
    for key, value in d2.items():
        if isinstance(value, dict) and key in d1:
            d1[key] = deep_merge(d1[key], value)  # Recurse into dict
        else:
            d1[key] = value  # If value is not a dict, overwrite/add
    return d1


def main():
    pass


if __name__ == "__main__":
    main()
