import datetime
import time

DATE_TIME_DEFAULT_FORMAT: str = "%Y-%m-%d %H:%M:%S"
DEFAULT_DATE: str = "2000-01-01"

def ts(time_str: str, date_str: str=None) -> float:
    """Get a timestamp for provided time.
        If 'date_str' is not provided let's use some fake year/month/day

        when is the exact, proper 'date_str' not needed ? ie: when
         - testing events in terms of minutes/hours of processing - but still within the same day
            what we care about are seconds/minutes etc.
        We can pass the string representing the date (but no validation etc. for now) - just in case for some potential future needs
    """
    date_info: str = date_str if date_str else DEFAULT_DATE
    date_with_time: str = f"{date_info} {time_str}"
    timestamp = time.mktime(
        datetime.datetime.strptime(date_with_time, DATE_TIME_DEFAULT_FORMAT).timetuple()
    )
    print(f"ts: {timestamp} for {date_with_time}")

    return timestamp
