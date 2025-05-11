import json
from datetime import datetime
from pathlib import Path
from hll_stats_tools.utils.logger_utils import setup_logger

logger = setup_logger(__name__)


def openfile(file: str | Path) -> dict:
    if isinstance(file, str):
        file = Path(file)
    with file.open("r", encoding="utf-8") as f:
        try:
            data = json.loads(f.read())
        except Exception as e:
            logger.error("Failed to load JSON from %s: %s", file, str(e))
            data = None
    return data


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
    except Exception as e:
        logger.error("Failed to recuperate datetime: %s", str(e))
        return None


def start_end_isostring(game: dict) -> list:
    return sorted(
        [
            x["event_time"]
            for x in game["logs"]
            if x["type"] == "MATCH START" or x["type"] == "MATCH ENDED"
        ]
    )
