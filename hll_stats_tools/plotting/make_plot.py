from datetime import date
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.nonparametric.smoothers_lowess import lowess


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
        df.loc[
            df["metric"] == "list Apolo kpm", "value"
        ] *= constant_multiplier

    # Apply date range filtering
    if lim_start is not None:
        df = df[df["date"] >= pd.to_datetime(lim_start)]
    if lim_end is not None:
        df = df[df["date"] <= pd.to_datetime(lim_end)]

    df["date"] = pd.to_datetime(
        df["date"], format="%Y-%m-%dT%H-%M-%S", errors="coerce"
    ).dt.tz_localize(None)
    df = df.dropna(subset=["date"])

    df["group"] = (
        df["date"].dt.to_period(conversions[timeframe_group_by]).dt.start_time
    )
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

    plt.title(
        f"Player: {player_name} - {timeframe_group_by.capitalize()}ly Metrics"
    )
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


def plot_multiple_metrics(
    metrics_by_date: dict[str, dict[str, float]],
    group_by: str = "W",  # 'D', 'W', 'M'
    rolling_average: int | None = None,
    display_rolling_average_overlay: bool = False,
    title: str = "",
    namefile: Path | None = None,
    min_value: float = 0.0,
    max_value: float = 1.75,
):
    """
    Plots one or more time series (player metrics) with optional resampling and
    rolling average.

    Parameters:
        metrics_by_date : dict[str, dict[str, float]]
            Format: {metric_name: {date_str: value, ...}}
        group_by : str
            Time grouping: 'D', 'W', or 'M'
        rolling_average : int | None
            If set, apply rolling average (overlay or replace)
        display_rolling_average_overlay : bool
            If True, plots both raw and rolling average on same chart
        title : str
            Title of the plot
        namefile : Path | None
            If set, saves the plot; otherwise, shows it
    """

    if not metrics_by_date:
        print("No data to plot.")
        return

    # Build base DataFrame
    df = pd.DataFrame(
        {
            metric: pd.Series(data).astype(float)
            for metric, data in metrics_by_date.items()
        }
    )
    df = df[df != 0]
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)

    # Resample (grouping)
    if group_by in {"D", "W", "M"}:
        df = df.resample(group_by).mean()

    # Create plot
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 6), dpi=300)
    ax.set_ylim(min_value, max_value)

    if rolling_average and display_rolling_average_overlay:
        # Plot raw lines first
        for column in df.columns:
            ax.plot(
                df.index,
                df[column],
                linestyle="--",
                linewidth=1,
                alpha=0.6,
                label=column,
            )

        # Add rolling averages as dashed lines
        rolling_df = df.rolling(window=rolling_average, min_periods=1).mean()
        for column in rolling_df.columns:
            ax.plot(
                rolling_df.index,
                rolling_df[column],
                linestyle="-",
                linewidth=1,
                alpha=0.6,
                label=f"{column} (avg)",
            )

    elif rolling_average:
        # Only show rolling average, not raw values
        rolling_df = df.rolling(window=rolling_average, min_periods=1).mean()
        for column in rolling_df.columns:
            ax.plot(
                rolling_df.index,
                rolling_df[column],
                linestyle="-",
                linewidth=2,
                label=f"{column} (avg)",
            )
    else:
        # Only raw values
        for column in df.columns:
            ax.plot(
                df.index,
                df[column],
                linestyle="--",
                linewidth=1.5,
                alpha=0.7,
                label=column,
            )

    ax.set_title(title or "Player Metrics Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Value")
    ax.legend(title="Metric")
    ax.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    if namefile:
        plt.savefig(namefile, bbox_inches="tight")
        print(f"Plot saved to: {namefile}")
        plt.close()
    else:
        plt.show()


def plot_scatter_metric_dates(
    df: pd.DataFrame,
    player_id: str,
    metric: str,
    player_name: str = "",
    round_to: int = 2,
    namefile: Path | None = None,
    out_folder: Path | None = None,
    min_value: float = 0.0,
    max_value: float = 2.0,
):

    # drop zero games
    df = df[df[metric] > 0]

    # prepare for plotting
    dates = df.index
    metric_values = df[metric].values

    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 6), dpi=300)
    ax.set_ylim(min_value, max_value)

    # scatter
    ax.scatter(
        dates,
        metric_values,
        marker="x",
        linewidths=0.7,
        s=5,
        alpha=0.7,
        label="Games",
    )

    # LOWESS smoothing
    x_numeric = mdates.date2num(dates)
    smoothed = lowess(metric_values, x_numeric, frac=0.2)
    ax.plot(
        mdates.num2date(smoothed[:, 0]),
        smoothed[:, 1],
        linewidth=2,
        color="grey",
        label="LOWESS",
    )

    # format x-axis for dates
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate()

    # labels, title, legend
    metric_name = metric.replace("_", " ").title()

    player_tag = f"{player_id}_"
    if player_name:
        player_tag = f"{player_name}_"

    ax.set_title(
        f"{player_tag}{metric_name} scatter by Game Date (LOWESS Smoothed)"
    )
    ax.set_xlabel("Game Date")
    ax.set_ylabel(metric_name)
    ax.legend()

    plt.tight_layout()

    if not namefile and out_folder:
        namefile = Path(out_folder) / Path(f"{player_tag}{metric}_scatter.png")

    if out_folder:
        plt.savefig(namefile, bbox_inches="tight")
        print(f"Plot saved to: {namefile}")
        plt.close()
    else:
        plt.show()


def main():
    pass


if __name__ == "__main__":
    main()
