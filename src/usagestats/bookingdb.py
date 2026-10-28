import multiprocessing as mp
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from sqlite3 import Error
from urllib import request

import pandas as pd
import pytz
from ics import Calendar


def daterange(start, end, step):
    """Range of dates generator

    Parameters
    ----------
    start: datetime
        start date
    end: datetime
        end date
    step: timedelta
        step size, e.g. timedelta(days=7)

    Yields
    -------
    datetime
        next date in the range
    """
    curr = start
    while curr < end:
        yield curr
        curr += step


def load_ics(args):
    """Load data from an ics calendar file

    Parameters
    ----------
    args: tuple
        (id, filename) where id is the instrument id and filename is the path to the ics file

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: guid, user, email, start, end, subject, duration, hours, instrument_id

    """
    id, filename = args
    local_tz = pytz.timezone("Europe/London")
    rec = []
    with open(filename) as f:
        c = Calendar(f.read())
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
                        "guid": e.uid,
                        "user": e.organizer.common_name,
                        "email": e.organizer.email,
                        "start": t0,
                        "end": t1,
                        "subject": subject,
                        "duration": t1 - t0,
                        "hours": (t1 - t0).total_seconds() / 3600,
                    }
                )
    df = pd.DataFrame(rec)
    df["instrument_id"] = id
    return df


class BookingDB:
    """Bookings class

    Represent a connection to the booking db

    use with a context manager
    ```
    with BookingDB('file.db') as bookings:
        bookings.create_booking_types(types)
    ```
    """

    def __init__(self, db_file, cookie=None, curl_str=None):
        """Create a BookingDB instance

        Parameters
        ----------
        db_file: str
            path to the sqlite database file
        cookie: str, optional
            cookie string to use for authentication, if None, it will be set from curl_str
        curl_str: str, optional
            curl command string to extract the cookie, if None, cookie must be provided
        """
        self.db_file = db_file
        self.cache_dir = Path(self.db_file).parent / "_cache"
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()
        self.cookie = cookie
        self.set_cookie_from_curl(curl_str)

    def __enter__(self):
        """Enter the context manager"""
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        """Exit the context manager"""
        self.cursor.close()
        if isinstance(exc_value, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()

    def calendar_to_instrument_name(self, x):
        """Convert a calendar name to an instrument name

        Parameters
        ----------
        x: str
            calendar name, e.g. "Calendar of resource 'Nikon W1 @mrc-lmb.cam.ac.uk'

        Returns
        -------
        str
            instrument name, e.g. 'Nikon W1'
        """
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

    def create_booking_types(self, booking_types):
        """Create the booking types table from a list of booking types

        Parameters
        ----------
        booking_types: list of str
            list of booking types, e.g. ["standard", "maintenance", "training", "service"]

        """
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS booking_types (
                id integer PRIMARY KEY,
                name text UNIQUE
                )"""
        )
        self.connection.commit()

        for t in booking_types:
            try:
                self.cursor.execute(
                    """INSERT INTO booking_types (name) VALUES (?)""",
                    (t,),
                )
                self.connection.commit()
            except Error:
                pass

    def create_division(self, divisions):
        """Create the divisions table from a list of divisions

        Parameters
        ----------
        divisions: list of str
            list of divisions, e.g. ["Division A", "Division B", "Division C"]
        """
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS divisions (
                id integer PRIMARY KEY,
                name text UNIQUE
            )"""
        )
        self.connection.commit()

        try:
            self.cursor.execute(
                """INSERT INTO divisions (id,name) VALUES (1,Unknown)"""
            )
        except Error:
            pass

        for t in divisions:
            try:
                self.cursor.execute(
                    """INSERT INTO divisions (name) VALUES (?)""",
                    (t,),
                )
                self.connection.commit()
            except Error:
                pass

    def get_division_id(self, name):
        """Get the division id by name

        Parameters
        ----------
        name: str
            name of the division, e.g. "Cell Biology

        Returns
        -------
        int
            id of the division, if not found returns 0
        """
        self.cursor.execute("""SELECT rowid FROM divisions WHERE name=?""", (name,))
        row = self.cursor.fetchone()
        if row is None:
            return 0
        else:
            return row[0]

    def create_groups(self, groups: pd.DataFrame):
        """Create the groups table from a dataframe

        Parameters
        ----------
        groups: pd.DataFrame
            dataframe with columns Group, Division
            e.g.:
            | Group      | Division       |
            |------------|----------------|
            | Munro      | Cell biology   |
            | Greger     | Neurobiology   |

        """
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS groups (
                id integer PRIMARY KEY,
                name text UNIQUE,
                division_id integer,
                FOREIGN KEY (division_id) REFERENCES divisions (id)
            )"""
        )
        self.connection.commit()

        try:
            self.cursor.execute(
                """INSERT INTO groups (id,name,division_id) VALUES (1,Unknown,1)"""
            )
        except Error:
            pass

        for t in groups.iloc:
            try:
                did = self.get_division_id(t["Division"])
                self.cursor.execute(
                    """INSERT INTO groups (name, division_id) VALUES (?,?)""",
                    (t["Group"], did),
                )
                self.connection.commit()
            except Error:
                pass

    def create_calendars_from_df(self, calendars):
        """Create the calendar table from a dataframe

        Parameters
        ----------
        calendars: pd.DataFrame
            dataframe with columns url, instrument, pc, confocal
            e.g.:
            | url                          | instrument | pc | confocal |
            |------------------------------|------------|----|----------|
            | http://example.com/calendar1 | Nikon W1   | 1  | 0        |
            | http://example.com/calendar2 | Zeiss LSM  | 0  | 1        |

        """
        # create the calendars table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS calendars (
                id integer PRIMARY KEY,
                url text UNIQUE,
                instrument text,
                pc integer,
                confocal integer
                )"""
        )
        self.connection.commit()
        # add the calendars
        for row in calendars.iloc:
            try:
                self.cursor.execute(
                    """INSERT INTO calendars (url, instrument, pc, confocal) VALUES (?,?,?,?)""",
                    (
                        row["url"],
                        row["instrument"],
                        int(row["pc"]),
                        int(row["confocal"]),
                    ),
                )
                self.connection.commit()
            except Error as e:
                print(e)
                pass

    def create_instruments(self):
        """Create the instruments table from the calendars table

        This will create a table with unique instrument names from the calendars table.
        """
        # create the instruments table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS instruments (
                id integer PRIMARY KEY,
                name text UNIQUE
                )"""
        )

        self.cursor.execute("""SELECT instrument FROM calendars""")

        # add the instruments
        rows = self.cursor.fetchall()
        for row in rows:
            try:
                self.cursor.execute(
                    """INSERT INTO instruments (name) VALUES (?)""",
                    (row[0],),
                )
                self.connection.commit()
            except Error as e:
                # print(e)
                pass

    def get_booking_type(self, x):
        """Get the type of booking by matching test from type list

        Parameters
        ----------
        x: str
            subject of the booking, e.g. "Jean-Paul", "Maintenance", "Training"

        Returns
        -------
        int
            id of the booking type, if not found returns 1 (standard booking)
        """
        self.cursor.execute(
            """SELECT rowid,name FROM booking_types WHERE name=?""", (x.lower(),)
        )
        row = self.cursor.fetchone()
        if row is None:
            return 1
        else:
            return row[0]

    def get_instrument_id(self, x):
        """Get the id of the instrument by name

        Parameters
        ----------
        x: str
            name of the instrument, e.g. "Nikon W1", "Zeiss LSM"

        Returns
        -------
        int
            id of the instrument, if not found returns 1 (default instrument)
        """
        self.cursor.execute("""SELECT rowid,name FROM instruments WHERE name=?""", (x,))
        row = self.cursor.fetchone()
        if row is None:
            return 1
        else:
            return row[0]

    def get_usage(self, week, bookings):
        """Get the amount of usage in hours per week

        Parameters
        ----------
        week: pd.Series
            Series with columns Start and End, representing the week
        bookings: pd.DataFrame
            DataFrame with columns start and end, representing the bookings

        Returns
        -------
        float
            total usage in hours for the week
        """
        print(week)
        s = 0.0
        for booking in bookings.iloc:
            d = (
                min(booking["end"], week["End"]) - max(booking["start"], week["Start"])
            ).total_seconds()
            if d > 0.0:
                s += d / 3600.0
        return s

    def get_weekly_usage(self, events, start_date, end_date):
        """Get the weekly usage in hours for a given date range

        Parameters
        ----------
        events: pd.DataFrame
            DataFrame with columns start and end, representing the bookings
        start_date: datetime
            start date of the range
        end_date: datetime
            end date of the range

        Returns
        -------
        pd.DataFrame
            DataFrame with columns Week, Start, End, Usage
            where Usage is the total usage in hours for each week
        """
        weeks_start = [d for d in daterange(start_date, end_date, timedelta(days=7))]
        df = pd.DataFrame(
            {
                "Week": pd.Series(range(1, len(weeks_start) + 1)),
                "Start": weeks_start,
            }
        )
        df["End"] = df["Start"] + timedelta(days=7)
        df["Usage"] = [self.get_usage(w, events) for w in df.iloc]
        return df

    def weekly_usage_by_instrument(self, start_date, end_date):
        events = self.get_events_in_range(start_date, end_date)
        usage = []
        for name, group in events.groupby("instrument"):
            u = self.get_weekly_usage(group, start_date, end_date)
            u["Instrument"] = name
            usage.append(u)
        return pd.concat(usage)

    def get_group_id(self, x):
        """Get the type of group by matching test from type list"""
        self.cursor.execute("""SELECT id FROM groups WHERE name=?""", (x,))
        row = self.cursor.fetchone()
        if row is None:
            return 0
        else:
            return row[0]

    def create_users(self, users):
        """Create the users table

        Parameters
        ----------
        users: pd.Dataframe
            dataframe with user, email, group
        """
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS users (
                id integer PRIMARY KEY,
                name text,
                group_id integer,
                email text UNIQUE,
                FOREIGN KEY (group_id) REFERENCES group_id (id)
            )"""
        )
        self.connection.commit()
        for u in users.iloc:
            try:
                gid = self.get_group_id(u["group"])
                name = u["user"]
                email = u["email"]
                self.cursor.execute(
                    """INSERT INTO users (name,group_id,email) VALUES (?,?,?)""",
                    (name, gid, email),
                )
                self.connection.commit()
            except Error:
                pass

    def get_unique_users_in_range(self, start_date, end_date):
        """
        Get the user table as dataframe

        """
        q = (
            """
        SELECT DISTINCT
            users.name,
            users.email,
            groups.name as 'group',
            divisions.name as 'division'
        FROM users
        INNER JOIN groups ON users.group_id=groups.id
        INNER JOIN divisions ON groups.division_id=divisions.id
        INNER JOIN events ON users.id=events.user_id
        WHERE start>date('"""
            + start_date.isoformat(sep=" ")
            + """') and
                end<date('"""
            + end_date.isoformat(sep=" ")
            + """') """
        )
        return pd.read_sql_query(q, self.connection)

    def get_user_id(self, email):
        """Get the type of group by matching test from type list"""
        self.cursor.execute("""SELECT id FROM users WHERE email=?""", (email,))
        row = self.cursor.fetchone()
        if row is None:
            return 0
        else:
            return row[0]

    def update_users_group_from_df(self, df):
        """update groups in user table from a dataframe with columns Group and User"""
        for u in df.iloc:
            gid = self.get_group_id(u["group"])
            self.cursor.execute(
                """UPDATE users
                   SET group_id = ?
                   WHERE name = ?""",
                (gid, u["user"]),
            )
            self.connection.commit()

    def get_events_in_range(self, start_date, end_date):
        """Get all events in date range

        Parameters
        ----------
        start_date: str
            start date as iso-format data yyyy-mm-dd
        end_date: str
            end date as iso-format data yyyy-mm-dd
        """

        q = (
            """
            SELECT 
                events.duration AS 'hours', 
                events.start AS 'start',
                events.end AS 'end',
                users.name AS 'user',
                users.email as 'email',
                groups.name AS 'group',
                divisions.name AS 'division',
                instruments.name as 'instrument',
                booking_types.name as 'type',
                events.subject
            FROM events 
            INNER JOIN users ON events.user_id=users.id
            INNER JOIN groups ON users.group_id=groups.id
            INNER JOIN divisions ON groups.division_id=divisions.id
            INNER JOIN instruments ON instrument_id=instruments.id
            INNER JOIN booking_types ON booking_type_id=booking_types.id
            WHERE start>date('"""
            + start_date.isoformat(sep=" ")
            + """') and
                  end<date('"""
            + end_date.isoformat(sep=" ")
            + """') """
        )

        events = pd.read_sql_query(
            q,
            self.connection,
        )

        events["start"] = pd.to_datetime(events["start"], utc=True)
        events["end"] = pd.to_datetime(events["end"], utc=True)
        return events

    def create_events(self, users):
        """Create the events table from the calendars

        Parameters
        ----------
        users: pd.DataFrame
            DataFrame with columns user, email, group, division
            e.g.:
            | user       | email                | group   | division       |
            |------------|----------------------|---------|----------------|
            | Jean-Paul  | jp@mrc-lmb.cam.ac.uk | Munro   | Cell Biology   |

        """
        columns = [
            "guid",
            "user",
            "email",
            "start",
            "end",
            "subject",
            "duration",
            "hours",
            "attendees",
            "instrument",
        ]
        # get the list of instruments
        self.cursor.execute("""SELECT instrument, url FROM calendars""")
        rows = self.cursor.fetchall()

        # load all calendars in parallel, but keep the database writes serial
        print("load cal")
        if not rows:
            df = pd.DataFrame(columns=columns)
        else:
            with ThreadPoolExecutor(max_workers=min(4, len(rows))) as pool:
                futures = []
                for i, row in enumerate(rows):
                    if i > 0:
                        time.sleep(1.0)
                    futures.append(pool.submit(self.load_calendar, row[0], row[1]))
                frames = []
                for future in as_completed(futures):
                    try:
                        res = future.result()
                        if res is not None and not res.empty:
                            frames.append(res)
                    except Exception as e:
                        print(f"Error loading calendar: {e}")
            if frames:
                df = pd.concat(frames, ignore_index=True)
            else:
                df = pd.DataFrame(columns=columns)

        # get the users from the list of events merge with known users
        all_users = (
            df[["email", "user"]]
            .drop_duplicates()
            .merge(users[["email", "group"]], how="left", on="email")
            .fillna("Unknown")
        )

        print("create users")
        self.create_users(all_users)

        # create event table
        # add UNIQUE(guid),
        # to ensure that there is inly one booking and one start at a time
        # sqliste does not have global unique id
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                guid TEXT UNIQUE,
                user_id INTEGER,
                instrument_id INTEGER,
                booking_type_id INTEGER,
                start TEXT,
                end TEXT,
                duration REAL,
                subject TEXT,
                FOREIGN KEY (user_id) REFERENCES users (id)
                FOREIGN KEY (instrument_id) REFERENCES instruments (id)
            )"""
        )
        self.connection.commit()

        # create the attendees table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS attendees (
                id INTEGER PRIMARY KEY,
                event_id INTEGER,
                user_id INTEGER,
                FOREIGN KEY (event_id) REFERENCES events (id)
                FOREIGN KEY (user_id) REFERENCES users (id)
            )"""
        )
        print("create events")
        # insert events
        duplicates = []
        for event in df.iloc:
            # check if the event already exists
            self.cursor.execute(
                """SELECT id FROM events WHERE guid=?""",
                (event["guid"],),
            )
            if self.cursor.fetchone() is not None:
                # if it exists, skip it
                duplicates.append(event["guid"])
                continue
            try:
                uid = int(self.get_user_id(event["email"]))
                iid = int(self.get_instrument_id(event["instrument"]))
                bid = int(self.get_booking_type(event["subject"]))
                self.cursor.execute(
                    """INSERT INTO events (
                        guid, 
                        user_id,
                        instrument_id,
                        booking_type_id,
                        start,
                        end,
                        duration,
                        subject
                        ) VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        event["guid"],
                        uid,
                        iid,
                        bid,
                        str(event["start"]),
                        str(event["end"]),
                        float(event["hours"]),
                        event["subject"],
                    ),
                )
                self.connection.commit()
            except Error:
                # print("error in events", error, event["guid"])
                duplicates.append(event["guid"])

            # insert attendees of the event to the attendees table
            # if the user is not in the users table, it will be skipped
            for attendee in event["attendees"]:
                uid = self.get_user_id(attendee)
                if uid > 0:
                    self.cursor.execute(
                        """INSERT INTO attendees (event_id, user_id) VALUES (?,?)""",
                        (event["guid"], uid),
                    )
                    self.connection.commit()

        return df, duplicates

    def load_calendar(self, instrument, url):
        """Load the calendar from a url into a dataframe

        Parameters
        ----------
        instrument: str
            name of the instrument, e.g. "Nikon W1"
        url: str
            url of the calendar, e.g. "http://example.com/calendar.ics

        Returns
        -------
        pd.DataFrame
            DataFrame with columns guid, user, email, start, end, subject, duration, hours, instrument
            where guid is the unique identifier of the event, user is the name of the organizer,
            email is the email of the organizer, start and end are the start and end times of the event,
            subject is the name of the event, duration is the duration of the event in timedelta,
            hours is the duration in hours, and instrument is the name of the instrument.

        Notes
        -----
        This function uses the cookie set in the BookingDB instance to authenticate the request.
        If the cookie is not set, it will raise an exception.
        """

        columns = [
            "guid",
            "user",
            "email",
            "start",
            "end",
            "subject",
            "duration",
            "hours",
            "attendees",
            "instrument",
        ]

        try:
            local_tz = pytz.timezone("Europe/London")
            cache_file = None
            if self.cache_dir:
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                sanitized_name = re.sub(r"[^\w\-_\. ]", "_", instrument).strip()
                cache_file = self.cache_dir / f"{sanitized_name}.ics"

            txt = None
            if cache_file and cache_file.exists():
                txt = cache_file.read_text(encoding="utf-8")
            else:
                req = request.Request(url)
                req.add_header("Cookie", self.cookie)
                with request.urlopen(req, timeout=15) as response:
                    txt = response.read().decode("utf-8")
                if cache_file and txt:
                    cache_file.write_text(txt, encoding="utf-8")

            # remove VALARM blocks
            pattern = r"BEGIN:VALARM.*?END:VALARM\n?"
            flt_txt = re.sub(pattern, "", txt, flags=re.DOTALL)

            c = Calendar(flt_txt)
            rec = []
            for e in c.events:
                if e.organizer is not None:
                    t0 = e.begin.datetime.replace(tzinfo=local_tz)
                    t1 = e.end.datetime.replace(tzinfo=local_tz)
                    if e.name is not None:
                        subject = e.name.lower().replace(
                            "maintenace", "maintenance"
                        )
                    else:
                        subject = "None"
                    rec.append(
                        {
                            "guid": e.uid,
                            "user": e.organizer.common_name,
                            "email": str(e.organizer.email).split("@")[0],
                            "start": t0,
                            "end": t1,
                            "subject": subject,
                            "duration": t1 - t0,
                            "hours": (t1 - t0).total_seconds() / 3600,
                            "attendees": [x.email for x in e.attendees],
                        }
                    )
            if not rec:
                return pd.DataFrame(columns=columns)
            df = pd.DataFrame.from_records(rec, columns=columns[:-1])
            df["instrument"] = instrument
            return df
        except Exception as e:
            print(
                f"Could not download {url}, update the cookie.txt file.\n"
                "Open the url, press ctrl-shift-I, press reload,\n "
                "right click on the lower panel, copy value as cURL\n"
            )
            return pd.DataFrame(columns=columns)

    def create_training_request(self):
        self.cursor.execute("""SELECT instrument, url FROM calendars""")
        rows = self.cursor.fetchall()
        frames = [self.load_training(r[0], r[1]) for r in rows]
        frames = [df for df in frames if df is not None and not df.empty]
        if frames:
            df = pd.concat(frames, ignore_index=True)
        else:
            df = pd.DataFrame(
                columns=[
                    "guid",
                    "user",
                    "email",
                    "start",
                    "end",
                    "subject",
                    "duration",
                    "hours",
                    "attendees",
                    "instrument",
                ]
            )
        return df

    def load_training(self, instrument, url):
        """Load the calendar from a url into a dataframe filtered for training requests and listing all attendees

        Parameters
        ----------
        instrument: str
            name of the instrument, e.g. "Nikon W1"
        url: str
            url of the calendar, e.g. "http://example.com/calendar.ics

        Returns
        -------
        pd.DataFrame
            DataFrame with columns guid, user, email, start, end, subject, duration, hours, instrument, attendees
            where guid is the unique identifier of the event, user is the name of the organizer,
            email is the email of the organizer, start and end are the start and end times of the event,
            subject is the name of the event, duration is the duration of the event in timedelta,
            hours is the duration in hours, instrument is the name of the instrument,
            and attendees is a list of emails of all attendees.

        Notes
        -----
        - This function uses the cookie set in the BookingDB instance to authenticate the request.
          If the cookie is not set, it will raise an exception.
        - Instead a attendees table should be created to store the attendees.
        """
        try:
            local_tz = pytz.timezone("Europe/London")
            cache_file = None
            if self.cache_dir:
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                sanitized_name = re.sub(r"[^\w\-_\. ]", "_", instrument).strip()
                cache_file = self.cache_dir / f"{sanitized_name}.ics"

            txt = None
            if cache_file and cache_file.exists():
                txt = cache_file.read_text(encoding="utf-8")
            else:
                req = request.Request(url)
                req.add_header("Cookie", self.cookie)
                with request.urlopen(req, timeout=15) as response:
                    txt = response.read().decode("utf-8")
                if cache_file and txt:
                    cache_file.write_text(txt, encoding="utf-8")

            rec = []
            c = Calendar(txt)
            for k, e in enumerate(c.events):
                    if e.organizer is not None:
                        t0 = e.begin.datetime.replace(tzinfo=local_tz)
                        t1 = e.end.datetime.replace(tzinfo=local_tz)
                        if e.name is not None:
                            if e.name.lower().find("training") >= 0:
                                rec.append(
                                    {
                                        "guid": e.uid,
                                        "user": e.organizer.common_name,
                                        "email": e.organizer.email,
                                        "start": t0,
                                        "end": t1,
                                        "subject": e.name,
                                        "duration": t1 - t0,
                                        "hours": (t1 - t0).total_seconds() / 3600,
                                        "attendees": [x.email for x in e.attendees],
                                    }
                                )
            if not rec:
                return pd.DataFrame(
                    columns=[
                        "guid",
                        "user",
                        "email",
                        "start",
                        "end",
                        "subject",
                        "duration",
                        "hours",
                        "attendees",
                        "instrument",
                    ]
                )
            df = pd.DataFrame.from_records(rec)
            df["instrument"] = instrument
            return df
        except Exception as e:
            print(f"Cound not download {url}")
            print(e)
            return pd.DataFrame(
                columns=[
                    "guid",
                    "user",
                    "email",
                    "start",
                    "end",
                    "subject",
                    "duration",
                    "hours",
                    "attendees",
                    "instrument",
                ]
            )

    def set_cookie_from_curl(self, curl_str):
        """Set the cookie from a curl string

        Parameters
        ----------
        curl_str: str
            curl command string, e.g. "curl -H 'Cookie: session=abc123' http://example.com/calendar.ics"

        Raises
        ------
        Exception
            if the cookie is not found in the curl string
        """
        if curl_str is not None:
            x = curl_str.split(" ")
            k = x.index("'Cookie:")
            if k > 0:
                self.cookie = " ".join(x[k + 1 : k + 3])
            else:
                raise Exception("Cookie not fount in curl string")
