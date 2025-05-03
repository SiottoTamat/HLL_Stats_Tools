from datetime import date

import matplotlib.pyplot as plt
import pandas as pd


def plot_player_data(
    df: pd.DataFrame,
    timeframe_group_by: str,
    lim_start: date | None = None,
    lim_end: date | None = None,
    metrics: str | list | None = None,
    drop_zeroes: bool = True,
    group_names: dict | None = None,
    constant_multiplier: int | None = None,
    rolling_av: str = "yes",
    namefile: str | None = None,
) -> None:
    assert timeframe_group_by in [
        "week",
        "month",
        "day",
    ], "timeframe_group_by must be 'week' or 'month'"
    if rolling_av:
        assert rolling_av in [
            "yes",
            "no",
            "both",
        ], "rolling_av must be 'yes', 'no', or 'both'"

    conversions = {"week": "W", "month": "M", "day": "D"}

    player_id = df["player_id"].unique()[0]
    player_name = player_id
    if group_names:
        player_name = group_names[player_id]

    if drop_zeroes:
        df = df[df.value != 0]

    if constant_multiplier:
        # df["value"] = df["value"] * constant_multiplier
        df.loc[df["metric"] == "list Apolo kpm", "value"] *= constant_multiplier

    # Apply date range filtering
    if lim_start is not None:
        df = df[df["date"] >= pd.to_datetime(lim_start)]
    if lim_end is not None:
        df = df[df["date"] <= pd.to_datetime(lim_end)]

    df["date"] = pd.to_datetime(
        df["date"], format="%Y-%m-%dT%H-%M-%S", errors="coerce"
    ).dt.tz_localize(None)
    df = df.dropna(subset=["date"])

    df["group"] = df["date"].dt.to_period(conversions[timeframe_group_by]).dt.start_time
    grouped = df.groupby(["group", "metric"])["value"].mean().reset_index()

    pivot_df = grouped.pivot(index="group", columns="metric", values="value")

    # Plot
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 6), dpi=300)
    if rolling_av == "no" or rolling_av == "both":
        pivot_df.plot(ax=ax, linewidth=1)

    if rolling_av == "yes" or rolling_av == "both":
        # Apply rolling average
        df["rolling_avg"] = (
            df.groupby(["group", "metric"])["value"]
            .rolling(window=3, center=True)
            .mean()
            .reset_index(level=[0, 1], drop=True)
        )
        rolling_df = pivot_df.rolling(window=3, center=True).mean()

        rolling_df.plot(
            ax=ax,
            linestyle="--",
            linewidth=1,
            alpha=0.6,
            label=[f"{col} (avg)" for col in rolling_df.columns],
        )

    plt.title(f"Player: {player_name} - {timeframe_group_by.capitalize()}ly Metrics")
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.grid(True)
    # plt.legend(title="Metric")
    # Modify the legend labels by removing 'list ' from the metric names
    handles, labels = plt.gca().get_legend_handles_labels()
    labels = [label.replace("list ", "") for label in labels]

    # Set the modified labels in the legend
    plt.legend(handles, labels, title="Metric")

    plt.tight_layout()
    if namefile:
        fig.savefig(namefile, format="png", dpi=300)
    else:
        plt.show()


def main():
    pass


if __name__ == "__main__":
    main()
