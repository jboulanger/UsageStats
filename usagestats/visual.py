import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz


def show_values_on_bars(axs, h_v="v", space=0.4, hspace=0.5):
    """from https://stackoverflow.com/questions/43214978/seaborn-barplot-displaying-values"""

    def _show_on_single_plot(ax):
        if h_v == "v":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height()
                value = int(p.get_height())
                ax.text(_x, _y, value, ha="center")
        elif h_v == "h":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() + float(space)
                _y = p.get_y() + p.get_height() - p.get_height() * hspace
                value = int(p.get_width())
                ax.text(_x, _y, value, ha="left")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _show_on_single_plot(ax)
    else:
        _show_on_single_plot(axs)


def get_title_str(bookings, start_date, end_date):
    total_usage_hrs = bookings["hours"].sum()
    num_users = len(bookings["user"].unique())
    num_insts = len(bookings["instrument"].unique())
    num_grps = len(bookings["group"].unique())
    info_str = f"\nFrom {start_date.date()} to {end_date.date()}\nGrand total: {total_usage_hrs:.0f} Hours\n #Users: {num_users} / #Groups: {num_grps} / #Instruments: {num_insts}"
    return info_str


def hours_per_instrument(bookings):
    ti = (
        bookings.groupby(["type", "instrument"], as_index=False)
        .agg(hours=pd.NamedAgg(column="hours", aggfunc="sum"))
        .pivot(index="instrument", values="hours", columns="type")
    )


def daterange(start, end, step):
    """Range of dates generator"""
    curr = start
    while curr < end:
        yield curr
        curr += step


def get_usage(week, events):
    """Get the amount of usage in hours per week"""
    s = 0.0
    for event in events.iloc:
        d = (
            min(event["end"], week["End"]) - max(event["start"], week["Start"])
        ).total_seconds()
        if d > 0.0:
            s += d / 3600.0
    return s


def get_weekly_usage(events, start_date, end_date):
    weeks_start = [d for d in daterange(start_date, end_date, timedelta(days=7))]
    df = pd.DataFrame(
        {
            "Week": pd.Series(range(1, len(weeks_start) + 1)),
            "Start": weeks_start,
        }
    )
    df["End"] = df["Start"] + timedelta(days=7)
    df["Usage"] = [get_usage(w, events) for w in df.iloc]
    return df


def weekly_usage_by_instrument(events, start_date, end_date):
    usage = []
    for name, group in events.groupby("instrument"):
        u = get_weekly_usage(group, start_date, end_date)
        u["Instrument"] = name
        usage.append(u)
    return pd.concat(usage)
