import pandas as pd
from datetime import datetime, timedelta
import pytz
import numpy as np
import re
from ics import Calendar
import multiprocessing as mp
from urllib import request


def daterange(start, end, step):
    """Range of dates generator"""
    curr = start
    while curr < end:
        yield curr
        curr += step


def get_booking_type(x, type_list):
    """Get the type of booking by matching test from type list"""
    for t in type_list:
        if x.lower().find(t) != -1:
            return t
    return "standard"


def to_date_time(str):
    """Convert a string to a datetime"""
    try:
        date = pd.to_datetime(str, format="%a %d/%m/%Y %H:%M")
    except (TypeError, ValueError):
        date = pd.to_datetime(str, format="%Y/%m/%d %H:%M:%S")
    local_tz = pytz.timezone("Europe/London")
    date.replace(tzinfo=local_tz)
    return date


def calendar_to_instrument_name(x):
    y = (
        x.stem.replace("Calendar of resource '", "")
        .replace("@mrc-lmb.cam.ac.uk'", "")
        .upper()
    )
    return (
        re.sub("(.*)_[0-9][N,S][0-9][0-9][0-9]", r"\1", y)
        .replace("_", " ")
        .capitalize()
    )


def load_instruments(file_list):
    calendars = [x for x in file_list]
    instruments = pd.DataFrame(
        {
            "Instrument": [calendar_to_instrument_name(x) for x in calendars],
            "Calendar": calendars,
        }
    )
    return instruments


def load_ics(filename, instrument_name, booking_types):
    """Load data from an ics calendar file
    Parameters
    ----------
    filename: str | pathlib.Path
        path to the ics file
    booking_types: list(str)
        list of the type of bookings
    Result
    ------
    bookings: pd.DataFrame
        dataframe with User, Start, End, Subject
    """
    local_tz = pytz.timezone("Europe/London")
    with open(filename) as f:
        c = Calendar(f.read())
        rec = []
        for k, e in enumerate(c.events):
            if e.organizer is not None:
                t0 = e.begin.datetime.replace(tzinfo=local_tz)
                t1 = e.end.datetime.replace(tzinfo=local_tz)
                if e.name is not None:
                    subject = e.name.lower().replace("maintenace", "maintenance")
                else:
                    subject = "None"
                rec.append(
                    {
                        "User": e.organizer.common_name,
                        "Start": t0,
                        "End": t1,
                        "Subject": subject,
                        "Duration": t1 - t0,
                        "Hours": (t1 - t0).total_seconds() / 3600,
                        "Type": get_booking_type(subject, booking_types),
                    }
                )
    df = pd.DataFrame(rec)
    df["Instrument"] = instrument_name
    return df


def load_excel(filename, instrument_name, type_list):
    """Load a xlsx file with booking information"""
    bookings = pd.read_excel(filename)
    bookings.rename(columns={"From": "User"}, inplace=True)
    bookings["Instrument"] = instrument_name
    bookings["Start"] = [to_date_time(x) for x in bookings["Start"]]
    bookings["End"] = [to_date_time(x) for x in bookings["End"]]
    bookings["Duration"] = bookings["End"] - bookings["Start"]
    bookings["Hours"] = [x.total_seconds() / 3600.0 for x in bookings["Duration"]]
    bookings["Subject"] = [
        x.lower().replace("maintenace", "maintenance") for x in bookings["Subject"]
    ]
    bookings["Type"] = [get_booking_type(x, type_list) for x in bookings["Subject"]]
    return bookings


def load_calendar(args):
    """Load bookings from a calendar either ics or xlsx file"""
    row, type_list = args
    if row["Calendar"].suffix == ".ics":
        return load_ics(row["Calendar"], row["Instrument"], type_list)
    elif row["Calendar"].suffix == ".xlsx":
        return load_excel(row["Calendar"], row["Instrument"], type_list)


def load_booking(instruments, type_list):
    """Load all bookings"""
    # create a list of tuples with instruments and type list
    tsks = [(x, type_list) for x in instruments.iloc]
    # create a pool of worker
    with mp.Pool() as pool:
        df = pool.map(load_calendar, tsks, chunksize=1)
    return pd.concat(df)


def filter_dates(bookings, start_date, num_weeks):
    local_tz = pytz.timezone("Europe/London")
    start = datetime.fromisoformat(start_date).replace(tzinfo=local_tz)
    stop = start + timedelta(weeks=num_weeks)
    return bookings[(bookings["Start"] > start) & (bookings["End"] < stop)]


def get_usage(week, bookings):
    """Get the amount of usage in hours per week"""
    s = 0.0
    for j, booking in bookings.iterrows():
        d = (
            min(booking["End"], week["End"]) - max(booking["Start"], week["Start"])
        ).total_seconds()
        if d > 0.0:
            s += d / 3600.0
    return s


def get_usage_by_instrument(bookings, start_date, num_weeks):
    """Get usage per instrument"""
    local_tz = pytz.timezone("Europe/London")
    start_date = datetime.fromisoformat(start_date).replace(tzinfo=local_tz)
    end_date = start_date + timedelta(weeks=num_weeks)
    weeks_start = [d for d in daterange(start_date, end_date, timedelta(days=7))]
    usage = []
    for intrument in bookings["Instrument"].unique():
        df = pd.DataFrame()
        df["Week"] = pd.Series(range(1, len(weeks_start) + 1))
        df["Start"] = weeks_start
        df["End"] = df["Start"] + timedelta(days=7)
        df["Instrument"] = intrument
        df["Usage"] = [
            get_usage(w, bookings[bookings["Instrument"] == intrument])
            for i, w in df.iterrows()
        ]
        usage.append(df)
    return pd.concat(usage, ignore_index=True)


def load_users(bookings, user_file):
    """List unique users from the bookings and list users"""
    unique_users = pd.DataFrame({"User": bookings["User"].unique()})
    unique_users.set_index("User")
    unique_users = unique_users[~unique_users["User"].isna()]
    print(f"There are {len(unique_users)} unique users listed in the bookings.")
    # merge with existing file
    if user_file.exists():
        print("Loading Users and Groups from files", user_file)
        tmp = pd.read_csv(user_file)
        tmp.set_index("User")
        # print additional users in bookin
        print(unique_users[~unique_users["User"].isin(tmp["User"])])
        users = pd.merge(unique_users, tmp, how="left", on="User")
    else:
        users = unique_users
        users["Group"] = "Unknown"

    users.sort_values(by="Group", inplace=True, ignore_index=True)
    users.fillna("Unknown", inplace=True)
    users = users[users["User"] != "Unknown"]
    n = len(users[users["Group"] == "Unknown"])
    print(f"Number of users without known group: {n}")
    if n > 0:
        print(f"Edit now the {user_file} to fill out missing groups.")
    return users


def load_groups(users, groups_file):
    """List unique groups from the bookings and list users"""
    unique_groups = pd.DataFrame({"Group": users["Group"].unique()})
    unique_groups.set_index("Group")
    # unique_groups['Division'] = 'Unknown'
    print(f"There are {len(unique_groups)} groups in the list of users.")
    # merge with existing file
    if groups_file.exists():
        print("Loading Groups from files", groups_file.stem)
        tmp = pd.read_csv(groups_file)
        tmp.set_index("Group")
        # print(unique_groups[~unique_groups['Group'].isin(tmp['Group'])])
        groups = pd.merge(unique_groups, tmp, how="left", on="Group")
    else:
        groups = unique_groups
        groups["Division"] = "Unknown"

    groups.sort_values(by="Division", inplace=True, ignore_index=True)
    groups.fillna("Unknown", inplace=True)
    n = len(groups[groups["Division"] == "Unknown"])
    print(f"Number of unknown division {n}")
    if n > 0:
        print(f"Edit now the {groups_file} to fill out missing groups.")
    return groups


def download_calendars(instruments_urls, cookie):
    """
    Download all calendars

    """
    for row in instruments_urls.iloc:
        req = request.Request(row["url"])
        req.add_header(cookie)
