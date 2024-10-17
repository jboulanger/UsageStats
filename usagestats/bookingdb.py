import sqlite3
from sqlite3 import Error
import re
from ics import Calendar
import pytz
import pandas as pd
import multiprocessing as mp


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

    def __init__(self, db_file):
        self.db_file = db_file
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()

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

    def create_instruments(self, instruments):
        """Create the instrument table from a dataframe"""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS instruments (
                id integer PRIMARY KEY,
                name text UNIQUE,
                path text
                )"""
        )
        self.connection.commit()

        for row in instruments.iloc:
            try:
                self.cursor.execute(
                    """INSERT INTO instruments (name, url) VALUES (?,?)""",
                    (self.calendar_to_instrument_name(fpath), str(fpath)),
                )
                self.connection.commit()
            except Error:
                pass  # print(e)

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
        self.cursor.execute("""SELECT rowid,name FROM intruments WHERE name=?""", (x,))
        row = self.cursor.fetchone()
        if row is None:
            return 1
        else:
            return row[0]

    def get_group_id(self, x):
        """Get the type of group by matching test from type list"""
        self.cursor.execute("""SELECT id FROM groups WHERE name=?""", (x,))
        row = self.cursor.fetchone()
        if row is None:
            return 0
        else:
            return row[0]

    def create_users(self, users):
        # create the users table
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
            gid = self.get_group_id(u["Group"])
            self.cursor.execute(
                """UPDATE users
                   SET group_id = ?
                   WHERE name = ?""",
                (gid, u["User"]),
            )
            self.connection.commit()

    def create_events(self):
        # get the list of instruments
        self.cursor.execute("""SELECT rowid, path FROM instruments""")
        rows = self.cursor.fetchall()

        # for each instrument load the calendar as a dataframe
        with mp.Pool() as pool:
            df = pool.map(load_ics, rows, chunksize=1)
        df = pd.concat(df)

        # get the users from the list of events
        users = df[["user", "email"]].drop_duplicates()
        users["group"] = "Unknown"
        self.create_users(users)

        # create event table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                instrument_id INTEGER,
                booking_type_id INTEGER,
                start TEXT,
                end TEXT,
                duration REAL,
                subject TEXT,
                UNIQUE(instrument_id, start),
                FOREIGN KEY (user_id) REFERENCES users (id)
                FOREIGN KEY (instrument_id) REFERENCES instruments (id)
            )"""
        )
        self.connection.commit()

        # insert events
        for e in df.iloc:
            try:
                uid = int(self.get_user_id(e["email"]))
                iid = int(e["instrument_id"])
                bid = int(self.get_booking_type(e["subject"]))
                self.cursor.execute(
                    """INSERT INTO events (
                        user_id,
                        instrument_id,
                        booking_type_id,
                        start,
                        end,
                        duration,
                        subject
                        ) VALUES (?,?,?,?,?,?,?)""",
                    (
                        uid,
                        iid,
                        bid,
                        str(e["start"]),
                        str(e["end"]),
                        float(e["hours"]),
                        e["subject"],
                    ),
                )
                self.connection.commit()
            except Error as e:
                print("error in events", e)

        return df
