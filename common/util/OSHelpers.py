#!/usr/bin/python

""" Helpers for operating system functions

Contains methods to get directory paths and other operating system functions.

"""

import os
from os.path import expanduser
from sys import platform


def get_user_home_dir():
    """ Get user's home directory

    Returns the user's home directory.

    Args:

    Returns:
        home_dir: (str) user's home directory

    """
    home_dir = expanduser("~")
    return home_dir


def get_curr_dir():
    """ Get the current working directory

    Returns the current working directory.

    Args:

    Returns:
        curr_dir: (str) current working directory

    """
    curr_dir = os.getcwd()
    return curr_dir


def create_dir(dir_path):
    """ Create a directory

    Creates a directory.

    Args:
        dir_path: (str) full path to the directory you'd like to create

    Returns:

    """
    try:
        os.mkdir(dir_path)
    except OSError:
        print("Creation of the directory %s failed!" % dir_path)
    else:
        print("Successfully created the directory %s " % dir_path)


def get_log_dir():
    """ Get the full path of the log directory

    Returns the directory where logs are stored.
    This is just the user's home directory > Log

    Returns:
        log_dir: (str) log directory

    """
    home_dir = get_user_home_dir()
    log_dir = home_dir + os.sep + 'Log'
    if os.path.isdir(log_dir)==False: # check to see if the Log directory exists, if not create it
        print("The log directory, %s, does not yet exist. Let's create it... " % log_dir)
        create_dir(log_dir)
    return log_dir


def get_log_filepath(app_name='Python App'):
    """ Get the full path of a log file

    Returns the filepath where logs are stored.
    This is just the user's home directory > Log > app_name.

    Args:
        app_name: (string) name of application

    Returns:
        log_filepath: (string) full path to the logging file

    """
    log_dir = get_log_dir()
    log_filepath = log_dir + os.sep + app_name
    return log_filepath


def get_operating_system():
    """ Get operating system

    Returns:
        operating_system (str): name of the operating system

    """
    operating_system = None
    if platform == "linux" or platform == "linux2":
        operating_system = "linux"
    elif platform == "darwin":
        operating_system = "osx"
    elif platform == "win32":
        operating_system = "windows"
    return operating_system

