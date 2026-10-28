from unittest.mock import patch

from usagestats.bookingdb import BookingDB


class DummyResponse:
    def __init__(self, text):
        self.text = text

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self.text.encode("utf-8")


def test_load_calendar_empty_calendar_has_expected_columns():
    db = BookingDB(":memory:")
    db.cookie = "session=abc"
    empty_ics = "BEGIN:VCALENDAR\nVERSION:2.0\nEND:VCALENDAR\n"

    with patch(
        "usagestats.bookingdb.request.urlopen", return_value=DummyResponse(empty_ics)
    ):
        df = db.load_calendar("Demo", "https://example.com/calendar.ics")

    assert list(df.columns) == [
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
    assert df.empty


def test_load_calendar():
    db = BookingDB(":memory:")
    db.cookie = "session=abc"
    with open("./tests/test.ics") as f:
        ics_txt = f.read()
    with patch(
        "usagestats.bookingdb.request.urlopen", return_value=DummyResponse(ics_txt)
    ):
        df = db.load_calendar("Demo", "./tests/test.ics")
        assert list(df.columns) == [
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
