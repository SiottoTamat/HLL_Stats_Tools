import json
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path

from hll_stats_tools.legacy_json.json_utils import (
    only_actual_game_logs,
    start_end_isostring,
)
from hll_stats_tools.legacy_json.logs_utils import check_game
from hll_stats_tools.utils import openfile


def game_analysis(game: dict, file_stem: str) -> dict:
    """
    Analyzes a game log and calculates various statistics related to kills, deaths, team kills,
    and player performance.

    Parameters:
    game (dict): A dictionary containing the game log data.

    Returns:
    dict: A dictionary containing various statistics related to the game.
    """
    start_date, map = file_stem.split("_")

    game_time, result = gametime_and_result(game)
    kills, deaths, teamkills = all_pl_kill_distribution(game)
    all_players = list_players(game)
    times_played = {x: player_total_seconds(game, x) for x in all_players}
    all_kpm = {
        x: get_event_per_minute(game, kills, x, times_played[x]) for x in all_players
    }
    all_dpm = {
        x: get_event_per_minute(game, deaths, x, times_played[x]) for x in all_players
    }
    all_tkpm = {
        x: get_event_per_minute(game, teamkills, x, times_played[x])
        for x in all_players
    }
    all_victims = {x: count_actor(kills, x) for x in all_players}
    all_nemesis = {x: count_actor(deaths, x) for x in all_players}
    tot_kills = {x: sum_values(all_victims, x) for x in all_players}
    tot_deaths = {x: sum_values(all_nemesis, x) for x in all_players}
    tot_tks = {x: count_events(teamkills, x) for x in all_players}
    tot_ratios = {
        x: round(tot_kills[x] / tot_deaths[x], 1) if tot_deaths[x] > 0 else 0
        for x in all_players
    }
    tot_weapon_kills = {
        x: {y[0]: y[1] for y in count_actor(kills, x, 1)} for x in all_players
    }
    tot_weapon_deaths = {
        x: {y[0]: y[1] for y in count_actor(deaths, x, 1)} for x in all_players
    }
    kills_average = kill_avg(tot_kills)
    all_Wkpm = {
        x: weighted_kpm(
            rank_percentile(all_kpm, x), tot_kills[x], kills_average, all_kpm[x]
        )
        for x in all_players
    }
    gfs = {x: Apolo_gf(all_kpm, times_played[x], game_time, x) for x in all_players}
    Apolo_kpm = {x: Apolo_GF(all_victims, tot_kills, x, gfs) for x in all_players}
    seeded = is_seeding(game)

    return {
        "start date": start_date,
        "date": start_date.split("T")[0],
        "map": map,
        "game time": game_time,
        "seeding match": seeded,
        "result allies": result[0],
        "result axis": result[1],
        "incomplete game": result[0] == result[1],
        "players": all_players,
        "players game time": times_played,
        "kills distribution": kills,
        "deaths distribution": deaths,
        "team kills distribution": teamkills,
        "list kpm": all_kpm,
        "list weighted kpm": all_Wkpm,
        "list Apolo kpm": Apolo_kpm,
        "list dpm": all_dpm,
        "list tkpm": all_tkpm,
        "list nemesis": all_nemesis,
        "list victims": all_victims,
        "total kills": tot_kills,
        "total deaths": tot_deaths,
        "total team kills": tot_tks,
        "total ratios": tot_ratios,
        "weapon kills": tot_weapon_kills,
        "weapon deaths": tot_weapon_deaths,
    }


def gametime_and_result(game: dict) -> int:
    start = None
    end = None
    result = None
    for item in game["logs"]:
        if item["type"] == "MATCH START":
            start = datetime.fromisoformat(item["event_time"])
        if item["type"] == "MATCH ENDED":
            end = datetime.fromisoformat(item["event_time"])
            result = [
                int(x.strip())
                for x in item["content"]
                .replace("(", "@@@")
                .replace(")", "@@@")
                .split("@@@")[1]
                .split(" - ")
            ]
    return ((end - start).total_seconds(), result)


def all_pl_kill_distribution(game: dict) -> tuple:
    """
    This function calculates the kill distribution and death distribution of players in a game.

    Parameters:
    game (dict): A dictionary representing a game log. The dictionary should contain the following keys:
        - "date": A string representing the start time of the game in ISO 8601 format.
        - "logs": A list of dictionaries representing game events. Each event dictionary should contain the following keys:
            - "type": A string representing the type of event (e.g., "KILL").
            - "event_time": A string representing the time of the event in ISO 8601 format.
            - "player1_id": A string representing the ID of the first player involved in the event.
            - "player2_id": A string representing the ID of the second player involved in the event.
            - "weapon": A string representing the weapon used in the event.

    Returns:
    tuple: A tuple containing two dictionaries. The first dictionary represents the kill distribution, where the keys are player IDs and the values are dictionaries. Each inner dictionary contains the seconds from the start of the game as keys and a list of tuples representing the victims and weapons used as values. The second dictionary represents the death distribution, following the same format as the kill distribution.
    """

    def check_situation(dict, seconds, actor, actor1, weapon) -> None:
        if actor1 == actor:
            print("here")
        if actor in dict.keys():  # and seconds in kill_distr[killer]:
            if seconds in dict[actor]:  # kill_distr[log["player1_id"]].append(seconds)
                dict[actor][seconds].append((actor1, weapon))
            else:
                dict[actor][seconds] = [(actor1, weapon)]
        else:
            # kill_distr[log["player1_id"]] = [seconds]
            dict[actor] = {seconds: [(actor1, weapon)]}

    match_start = game["date"]
    kill_distr = {}
    death_distr = {}
    team_kills = {}
    for log in [x for x in game["logs"] if x["type"] == "KILL"]:
        seconds = seconds_from_start(match_start, log["event_time"])
        killer = log["player1_id"]
        victim = log["player2_id"]
        weapon = log["weapon"]
        check_situation(kill_distr, seconds, killer, victim, weapon)
        check_situation(death_distr, seconds, victim, killer, weapon)
    for log in [x for x in game["logs"] if x["type"] == "TEAM KILL"]:
        seconds = seconds_from_start(match_start, log["event_time"])
        killer = log["player1_id"]
        victim = log["player2_id"]
        weapon = log["weapon"]
        check_situation(team_kills, seconds, killer, victim, weapon)
    return kill_distr, death_distr, team_kills


def list_players(game: dict) -> list:
    """
    This function retrieves a list of unique players from a game log.

    Parameters:
    game (dict): A dictionary representing a game log. The dictionary should contain the following keys:
        - "logs": A list of dictionaries representing game events. Each event dictionary should contain the following keys:
            - "player1_id": A string representing the ID of the first player involved in the event.
            - "player2_id": A string representing the ID of the second player involved in the event.
            - "player1_name": A string representing the name of the first player involved in the event.
            - "player2_name": A string representing the name of the second player involved in the event.

    Returns:
    list: A list of unique player names from the game log.
    """
    players = {}

    list_pl1 = [
        (x["player1_id"], x["player1_name"]) for x in game["logs"] if x["player1_id"]
    ]
    list_pl2 = [
        (x["player2_id"], x["player2_name"]) for x in game["logs"] if x["player2_id"]
    ]
    for player in set(list_pl1 + list_pl2):
        if player[0] in players.keys():
            players[player[0]].append(player[1])
        else:
            players[player[0]] = [player[1]]
    return players


def seconds_from_start(start_time, event_time) -> int:
    """
    Calculates the number of seconds between a start time and an event time.

    Parameters:
    start_time (str): A string representing the start time in ISO 8601 format.
    event_time (str): A string representing the event time in ISO 8601 format.

    Returns:
    int: The number of seconds between the start time and the event time.
    """
    start_time = datetime.fromisoformat(start_time)
    event_time = datetime.fromisoformat(event_time)
    return int((event_time - start_time).total_seconds())


def player_total_seconds(game: dict, player_id: str) -> int:
    """
    Calculates the total number of seconds a player was connected to the game.

    Parameters:
    game (dict): A dictionary representing a game log. The dictionary should contain the following keys:
        - "date": A string representing the start time of the game in ISO 8601 format.
        - "logs": A list of dictionaries representing game events. Each event dictionary should contain the following keys:
            - "type": A string representing the type of event (e.g., "DISCONNECTED", "CONNECTED").
            - "event_time": A string representing the time of the event in ISO 8601 format.
            - "player1_id": A string representing the ID of the player involved in the event.

    player_id (str): The ID of the player for whom the total connected time needs to be calculated.

    Returns:
    int: The total number of seconds the player was connected to the game.
    """
    start, end = start_end_isostring(game)
    game = only_actual_game_logs(game)
    player_connections = [
        (x["type"], x["event_time"])
        for x in game["logs"]
        if (
            (x["type"] == "DISCONNECTED" or x["type"] == "CONNECTED")
            and (x["player1_id"] == player_id)
        )
    ]
    if not player_connections:
        player_connections = [("CONNECTED", start), ("DISCONNECTED", end)]
    else:
        if (
            player_connections[0][0] == "DISCONNECTED"
        ):  # player connected at the beginning, left in the middle
            player_connections.insert(0, ("CONNECTED", start))
        if (
            player_connections[-1][0] == "CONNECTED"
        ):  # player connected in the middle, never left before the end of the match
            player_connections.append(("DISCONNECTED", end))
        #
    timed_tuples = [(x[0], datetime.fromisoformat(x[1])) for x in player_connections]

    paired = list(zip(timed_tuples[::2], timed_tuples[1::2]))
    total_seconds = 0
    for pair in paired:
        total_seconds += (pair[1][1] - pair[0][1]).total_seconds()

    return total_seconds


def get_event_per_minute(game, events, player_id, time_played: int = None) -> float:
    """
    Calculates the average number of events per minute for a specific player in a given event distribution.

    Parameters:
    game (dict): A dictionary representing the game log. The dictionary should contain the following keys:
        - "date": A string representing the start time of the game in ISO 8601 format.
        - "logs": A list of dictionaries representing game events. Each event dictionary should contain the following keys:
            - "type": A string representing the type of event (e.g., "KILL").
            - "event_time": A string representing the time of the event in ISO 8601 format.
            - "player1_id": A string representing the ID of the first player involved in the event.
            - "player2_id": A string representing the ID of the second player involved in the event.
            - "weapon": A string representing the weapon used in the event.

    events (dict): A dictionary representing the event distribution. The keys are player IDs, and the values are dictionaries. Each inner dictionary contains the seconds from the start of the game as keys and a list of tuples representing the event details as values.

    player_id (str): The ID of the player for whom the average number of events per minute needs to be calculated.

    time_played (int, optional): The total number of seconds the player was connected to the game. If not provided, it will be calculated using the `player_total_seconds` function.

    Returns:
    float: The average number of events per minute for the specified player in the given event distribution. The result is rounded to two decimal places.
    """
    if not time_played:
        time_played = player_total_seconds(game, player_id)
    all_events = total_events(events, player_id)
    minutes = time_played / 60
    if minutes:
        return round(all_events / minutes, 2)
    return 0


def total_events(event_distr: dict, player_id: str) -> int:
    """
    Calculates the total number of events (e.g., kills, deaths) for a specific player in a given event distribution.

    Parameters:
    event_distr (dict): A dictionary representing the event distribution. The keys are player IDs, and the values are dictionaries. Each inner dictionary contains the seconds from the start of the game as keys and a list of tuples representing the event details as values.
    player_id (str): The ID of the player for whom the total number of events needs to be calculated.

    Returns:
    int: The total number of events for the specified player in the given event distribution.
    """
    return sum(len(x) for x in event_distr.get(player_id, {}).values())


def count_actor(event_distr: dict, player_id: str, index: int = 0) -> dict:
    """
    Counts the occurrences of actors (players) in a given event distribution.

    Parameters:
    event_distr (dict): A dictionary representing the event distribution. The keys are player IDs, and the values are dictionaries. Each inner dictionary contains the seconds from the start of the game as keys and a list of tuples representing the event details as values.
    player_id (str): The ID of the player for whom the actor occurrences need to be counted.
    index (int, optional): An integer representing the index of the actor in the event details tuple. If 0, it counts the occurrences of the first actor in the tuple. If 1, it counts the occurrences of the second actor in the tuple. Defaults to 0.

    Returns:
    dict: A dictionary containing the actor IDs as keys and their corresponding counts as values. The dictionary is sorted in descending order based on the counts.
    """
    if player_id in event_distr:
        values = event_distr[player_id]
        if index == 0:
            all_ids = [x for events in values.values() for x, _ in events]
        if index == 1:
            all_ids = [x for events in values.values() for _, x in events]
        sums = dict(Counter(all_ids))
        return sorted(sums.items(), key=lambda x: x[1], reverse=True)
    return {}


def sum_values(event_distr: dict, player_id: str) -> int:
    """
    Calculates the sum of values associated with a specific player in an event distribution.

    Parameters:
    event_distr (dict): A dictionary representing the event distribution. The keys are player IDs, and the values are dictionaries. Each inner dictionary contains the seconds from the start of the game as keys and a list of tuples representing the event details as values.
    player_id (str): The ID of the player for whom the sum of values needs to be calculated.

    Returns:
    int: The sum of values associated with the specified player in the given event distribution. If the player ID is not found in the event distribution, the function returns 0.
    """
    if player_id in event_distr:
        values = event_distr[player_id]
        all_ids = [x[1] for x in values]
        return sum(all_ids)

    return 0


def count_events(event_distr: dict, player_id: str) -> int:
    """
    Counts the total number of events (e.g., kills, deaths) for a specific player in a given event distribution.

    Parameters:
    event_distr (dict): A dictionary representing the event distribution. The keys are player IDs, and the values are dictionaries. Each inner dictionary contains the seconds from the start of the game as keys and a list of tuples representing the event details as values.

    player_id (str): The ID of the player for whom the total number of events needs to be calculated.

    Returns:
    int: The total number of events for the specified player in the given event distribution. If the player ID is not found in the event distribution, the function returns 0.
    """
    if player_id in event_distr:
        values = event_distr[player_id]
        return sum([len(x) for x in values.values()])
    return 0


def kill_avg(tot_kills: dict) -> float:
    if tot_kills:
        return statistics.mean(tot_kills.values())
    return 0.0


def weighted_kpm(
    rank_percentile: float, player_kills: int, kill_avg: float, kpm: float
) -> float:
    """
    Calculates a weighted Kills per Minute (KPM) score based on the player's rank percentile,
    actual kills, average kills per minute, and the player's KPM.

    Parameters:
    rank_percentile (float): The player's rank percentile in the game.
    player_kills (int): The actual number of kills made by the player.
    kill_avg (float): The average number of kills per minute made by the players of the match.
    kpm (float): The player's Kills per Minute (KPM).

    Returns:
    float: The weighted KPM score.
    """
    if kill_avg > 0:
        return (1 - ((rank_percentile - 1) / 100)) * (player_kills / kill_avg) * kpm
    return 0


def rank_percentile(all_kpm: dict, player: str) -> float:
    score = all_kpm[player]
    return sum(1 for v in all_kpm.values() if v < score) / len(all_kpm)


def Apolo_gf(
    total_kpm: dict, total_seconds: int, time_game: int, player_id: str, A: float = 0.45
) -> float:
    player_id = str(player_id)
    kpm = total_kpm[player_id]
    # minutes_played = total_seconds

    return (total_seconds / time_game) ** A * kpm


def Apolo_GF(all_victims, total_kills, player_id: str, gfs: dict, B: float = 0.65):
    kills = total_kills[player_id]
    if kills > 0:
        sum_gf = sum(
            [get_gfs(gfs, idx) * value for idx, value in all_victims[player_id]]
        )
        return (sum_gf / kills) ** B * gfs[player_id]
    return 0


def get_gfs(gfs, player_id):
    if player_id in gfs:
        return gfs[player_id]
    return 0


def is_seeding(game: dict) -> bool:
    counter = 0
    for item in game["logs"]:
        if item["type"] == "MESSAGE" and "THANK YOU FOR SEEDING" in item["content"]:
            counter += 1
    if counter > 3:
        return True
    return False


def refill_analysis_folder(out_folder_analysis: str | Path, folder_games: str | Path):
    out_folder_analysis = Path(out_folder_analysis)
    last_analysis_file = sorted(out_folder_analysis.glob("*.json"))[-1]
    last_game_file_analysed = f"{last_analysis_file.stem.replace('_ANALYSIS','')}.json"
    for file in folder_games.glob("*.json"):
        if file.name > last_game_file_analysed:
            print(file.name)
            new_analysis_file = out_folder_analysis / f"{file.stem}_ANALYSIS.json"
            game = openfile(file)
            if not check_game(game):
                analysis = game_analysis(game, file.stem)
                with new_analysis_file.open("w", encoding="utf-8") as f:
                    json.dump(analysis, f)


def main():
    pass


if __name__ == "__main__":
    main()
