#!/usr/bin/python

""" Helper methods for manipulating dates, times and datetimes
"""

import datetime


def get_curr_date_str(date_format="%Y-%m-%d"):
    """ Get the current date as a string

    Returns:
        date_str: (str) current date

    """
    date_str = datetime.date.today().strftime(date_format)
    return date_str


def get_curr_datetime_str(date_format="%Y-%m-%d %H:%M:%S"):
    """ Get the current datetime as a string
    
    Returns:
        datetime_str: (str) current datetime

    """
    datetime_str = datetime.datetime.now().strftime(date_format)
    return datetime_str


def add_days_to_date_str(date_in_str, days_to_add, date_format='%Y-%m-%d'):
    """ Adds days to a date string

    Args:
        date_in_str: (str) input date
        days_to_add: (int) days to add to the input date
        date_format: (str) format of the input and return date

    Returns:
        date_out_str: (str) output date

    """
    date_in_dt = datetime.datetime.strptime(date_in_str, date_format)
    time_increment = datetime.timedelta(days=days_to_add)
    date_out_dt = date_in_dt + time_increment
    date_out_str = date_out_dt.strftime(date_format)
    return date_out_str


def subtract_days(date_init_str, date_final_str, date_format='%Y-%m-%d'):
    """ Returns date_final_str - date_init_str in days

    Args:
        date_init_str: (str) initial date
        date_final_str: (str) final date
        date_format: (str) format of the input dates

    Returns:
        time_delta.days: (int) difference between the initial and final dates in days

    """
    date_init_dt = datetime.datetime.strptime(date_init_str, date_format)
    date_final_dt = datetime.datetime.strptime(date_final_str, date_format)
    time_delta = date_final_dt - date_init_dt
    return time_delta.days


def is_date1_lteq_date2(date1_str, date2_str, date_format='%Y-%m-%d'):
    """ Returns whether date1 <= date2 is True or False

    Args:
        date1_str: (str) date on left hand side of <=
        date2_str: (str) date on right hand side of <=
        date_format: (str) format of the input dates

    Returns:
        out_bool: (bool) indicates whether date1 <= date2 is True or False
    """
    date1_dt = datetime.datetime.strptime(date1_str, date_format)
    date2_dt = datetime.datetime.strptime(date2_str, date_format)
    out_bool = date1_dt <= date2_dt
    return out_bool


def get_month_of_date(date_str, date_format='%Y-%m-%d'):
    """ Returns the month of date

    Args:
        date_str (str): input date
        date_format (str): format of the input date

    Returns:
        date_dt.month (int): month of year

    """
    date_dt = datetime.datetime.strptime(date_str, date_format)
    return date_dt.month


def get_year_of_date(date_str, date_format='%Y-%m-%d'):
    """ Returns the year of a date

    Args:
        date_str (str): input date
        date_format (str): format of the input date

    Returns:
        date_dt.year (int): year

    """
    date_dt = datetime.datetime.strptime(date_str, date_format)
    return date_dt.year
