import sqlite3
from sqlite3 import Error
import re
from ics import Calendar
import pytz
from contextlib import closing


class BookingDB:
    def __init__(self, db_file):
        self.db_file = db_file
        self.connection = sqlite3.connect(self.db_file)
        self.cur = self.connection.cursor()

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
        with closing(sqlite3.connect(self.db_file)) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(
                    """CREATE TABLE IF NOT EXISTS booking_types (name text UNIQUE)"""
                )
                connection.commit()

                for t in booking_types:
                    try:
                        self.cur.execute(
                            """INSERT INTO booking_types (name) VALUES (?)""",
                            (t,),
                        )
                        self.connection.commit()
                    except Error as e:
                        print(e)

    def create_instruments(self, file_list):
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS instruments (name text UNIQUE, path text)"""
        )
        self.connection.commit()

        for fpath in file_list:
            try:
                self.cur.execute(
                    """INSERT INTO instruments (name,path) VALUES (?,?)""",
                    (self.calendar_to_instrument_name(fpath), str(fpath)),
                )
                self.connection.commit()
            except Error:
                pass

    def get_booking_type(self, x):
        """Get the type of booking by matching test from type list"""
        self.cur.execute(
            """SELECT rowid,name FROM booking_types WHERE name==""", (x.lower(),)
        )
        rows = self.cur.fetchall()
        if not rows:
            return 0
        else:
            return rows[0][0]

    def load_ics(self, filename, instrument_name):
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
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS events (user_id text, start text, end text, subject text, duration int, hours int, booking_type_id int)"""
        )
        self.connection.commit()
        local_tz = pytz.timezone("Europe/London")
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
                    data = {
                        "user": e.organizer.common_name,
                        "start": t0,
                        "end": t1,
                        "subject": subject,
                        "duration": t1 - t0,
                        "hours": (t1 - t0).total_seconds() / 3600,
                        "booking_type_id": self.get_booking_type(subject),
                    }
        df = pd.DataFrame(rec)
        df["Instrument"] = instrument_name
        return df
