"""

    Often required functions to grab

"""
from datetime import date, timedelta
import calendar

def get_dates_in_year(year):
    start = date(year-1, 12, 1)
    end = date(year, 11, 30)
    delta = timedelta(days=1)
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y%m%d'))
        current += delta
    return dates

def get_date_from_years(y0, y1, month, day):
    """
    Returns a list of dates in yyyymmdd format for each year between start_year and end_year (inclusive)
    for the specified month and day.
    """
    from datetime import date
    dates = []
    for year in range(y0, y1 + 1):
        try:
            d = date(year, month, day)
            dates.append(d.strftime('%Y%m%d'))
        except ValueError:
            continue  # skip invalid dates
    return dates

def get_days_in_month(year, month):
    num_days = calendar.monthrange(year, month)[1]
    return [day for day in range(1, num_days + 1)]