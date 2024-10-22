import sqlite3
from sqlite3 import Error
import re
from ics import Calendar
from datetime import datetime, timedelta
import pytz
import pandas as pd
import multiprocessing as mp
from urllib import request
from functools import partial


def daterange(start, end, step):
    """Range of dates generator"""
    curr = start
    while curr < end:
        yield curr
        curr += step


def load_ics(args):
    """Load data from an ics calendar file"""
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

    use with a context manager:
    ```
    with BookingDB('file.db') as bookings:
        bookings.create_booking_types(types)
    ```
    """

    def __init__(self, db_file, cookie=None, curl_str=None):
        self.db_file = db_file
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()
        self.cookie = cookie
        self.set_cookie_from_curl(curl_str)

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.cursor.close()
        if isinstance(exc_value, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()

    def calendar_to_instrument_name(self, x):
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
        self.cursor.execute("""SELECT rowid FROM divisions WHERE name=?""", (name,))
        row = self.cursor.fetchone()
        if row is None:
            return 0
        else:
            return row[0]

    def create_groups(self, groups: pd.DataFrame):
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
        """Create the calendar table from a dataframe"""
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
        """Create the instruments table from the calendars table"""

        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS instruments (
                id integer PRIMARY KEY,
                name text UNIQUE
                )"""
        )

        self.cursor.execute("""SELECT instrument FROM calendars""")
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
                pass  #

    def get_booking_type(self, x):
        """Get the type of booking by matching test from type list"""
        self.cursor.execute(
            """SELECT rowid,name FROM booking_types WHERE name=?""", (x.lower(),)
        )
        row = self.cursor.fetchone()
        if row is None:
            return 1
        else:
            return row[0]

    def get_instrument_id(self, x):
        """Get the type of booking by matching test from type list"""
        self.cursor.execute("""SELECT rowid,name FROM instruments WHERE name=?""", (x,))
        row = self.cursor.fetchone()
        if row is None:
            return 1
        else:
            return row[0]

    def get_usage(self, week, bookings):
        """Get the amount of usage in hours per week"""
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
        """
        create the users table

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
        # get the list of instruments
        self.cursor.execute("""SELECT instrument, url FROM calendars""")
        rows = self.cursor.fetchall()

        # load all calendars as a dataframe
        # with mp.Pool() as pool:
        #    df = pool.map(partial(load_calendar, cookie=self.cookie), rows, chunksize=1)
        #
        #    df = pd.concat(df)
        df = pd.concat(
            [self.load_calendar(r[0], r[1]) for r in rows], ignore_index=True
        )

        # get the users from the list of events merge with known users
        all_users = (
            df[["user", "email"]]
            .drop_duplicates()
            .merge(users, how="left", on="user")
            .fillna("Unknown")
        )

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

        # insert events
        duplicates = []
        for event in df.iloc:
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
            except Error as error:
                # print("error in events", error, event["guid"])
                duplicates.append(event["guid"])

        return df, duplicates

    def load_calendar(self, instrument, url):
        print(instrument)
        try:
            local_tz = pytz.timezone("Europe/London")
            req = request.Request(url)
            req.add_header("Cookie", self.cookie)
            rec = []
            with request.urlopen(req) as response:
                txt = response.read().decode("utf-8")
                c = Calendar(txt)
                for k, e in enumerate(c.events):
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
                                "email": e.organizer.email,
                                "start": t0,
                                "end": t1,
                                "subject": subject,
                                "duration": t1 - t0,
                                "hours": (t1 - t0).total_seconds() / 3600,
                            }
                        )
            df = pd.DataFrame.from_records(rec)
            df["instrument"] = instrument
            return df
        except Exception as e:
            print(f"Cound not download {url}")
            print(e)
            return None

    def set_cookie_from_curl(self, curl_str):
        if curl_str is not None:
            x = curl_str.split(" ")
            k = x.index("'Cookie:")
            if k > 0:
                self.cookie = " ".join(x[k + 1 : k + 3])
            else:
                raise Exception("Cookie not fount in curl string")
