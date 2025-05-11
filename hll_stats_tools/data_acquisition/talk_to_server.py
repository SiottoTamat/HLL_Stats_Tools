import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union

import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from hll_stats_tools.utils.common_utils import openfile

load_dotenv(".env")
API_KEY = os.getenv("API_KEY")


def get_historical_log(
    from_: str, till: str, limit: int = 3000
) -> Union[Dict[str, Any], List[Any]]:
    """
    Fetches historical logs from the server within the specified date range and limit.

    Args:
        from_ (str): The start date-time in ISO format.
        till (str): The end date-time in ISO format.
        limit (int, optional): The maximum number of logs to fetch. Defaults to 3000.

    Returns:
        Union[Dict[str, Any], List[Any]]:
        The historical logs data as a dictionary or list.
    """

    # Convert the start date-time from string to datetime object
    if from_:
        _from = datetime.fromisoformat(from_)

    # Convert the end date-time from string to datetime object
    if till:
        till = datetime.fromisoformat(till)

    # Set up query parameters for the API request
    params = {
        "from_": _from,
        "till": till,
        "limit": limit,
    }

    # Set up headers for the API request, including authorization
    headers = {
        "Authorization": f"Bearer: {API_KEY}",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
    }

    # Define the API command to fetch historical logs
    command = "get_historical_logs"

    # Send the request to the server and get the data
    received = requests.get(
        f"https://gw-stats.hlladmin.com/api/{command}",
        params=params,
        headers=headers,
    )

    # Parse the response text as JSON
    data = json.loads(received.text)

    return data


def get_last_log_entry(folder: str | Path) -> str | None:
    """
    Find the most recent log file and get the time of the last log entry.

    Args:
        folder: The folder to search for log files.

    Returns:
        The time of the last log entry in the most recent log file,
        or None if no log files are found.
    """
    if isinstance(folder, str):
        folder = Path(folder)
    # Get a sorted list of all .json files in the folder
    files = sorted(folder.glob("*.json"))
    # If there are any files, get the last one
    file = files[-1] if files else None
    if file:
        # Open the file and read its contents
        logs = openfile(file)
        # Get the last log entry
        last_log = logs[-1]
        # Return the time of the last log entry
        return last_log["event_time"]
    # If no files were found, return None
    return None


def download_sequential_logs(folder: str | Path, _from: str, till):
    """
    Download logs from server and save them to files in a folder.

    Downloads logs from the server, starting from the given start date-time,
    and saves them to files in the given folder. The files are named after
    the date-time of the first log entry in the file, with the colons replaced
    by hyphens. The logs are sorted by event time before being saved.

    Args:
        folder: The folder to save the logs to.
        _from: The start date-time to download logs from.
        till: The end date-time to download logs up to.
    """
    while _from < till:

        start_from_file = get_last_log_entry(folder)
        if start_from_file:
            _from = start_from_file

        namefile = _from.replace(":", "-")
        out_file = folder / Path(f"{namefile}.json")
        date_to = (datetime.fromisoformat(_from) + relativedelta(days=1)).isoformat()
        data = get_historical_log(_from, date_to, limit=40000)
        with out_file.open("w", encoding="utf-8") as fout:
            out_data = sorted(data["result"], key=lambda x: x["event_time"])
            json.dump(out_data, fout, indent=4)
        print(f"Saved {len(data['result'])} logs to {out_file}")


def main():
    pass


if __name__ == "__main__":
    main()
