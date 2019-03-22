#!/usr/bin/python

""" Custom implementation of Python's default logging class

Contains a class used for logging.
It uses Python's standard logging class and initializes a logger object for your application.

*To do: we want to start using loguru... https://github.com/Delgan/loguru

"""

from common.util.OSHelpers import get_log_filepath

import logging
import re


LOG_LEVEL_DICTIONARY = {'DEBUG': 10, 'INFO': 20, 'WARN': 30, 'WARNING': 30, 'ERROR': 40}


class Logging:
    """ Custom implementation of Python's default logging class
    TODO: change the name of this class to Logger - it makes more sense.
    """

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARN = logging.WARN
    ERROR = logging.ERROR

    def __init__(self, name, log_filename=None, log_level_str='INFO'):
        """ Create a common.Util.Logging.Logging object

        Sets up a logger to be used across a Python application.

        Args:
            name: (str) name of function/class
            log_filename: (str) name of log file to write to
            log_level_str: (str) log level

        Returns:

        Example:
            from Util.OSHelpers import get_log_filepath
            from Util.Logging import Logging
            logging_obj = Logging(__name__, log_filename=get_log_filepath('GoogleAnalytics')+__name__)
        """
        if log_filename is None:
            log_filename = get_log_filepath()

        self.log_filename = log_filename + '.log'

        self.log_level = LOG_LEVEL_DICTIONARY[log_level_str]
        logging.basicConfig(level=self.log_level)

        logger = logging.getLogger(name)
        logger.setLevel(self.log_level)
        handler = logging.FileHandler(self.log_filename)
        handler.setLevel(self.log_level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        handler.setFormatter(formatter)
        logger.handlers = []
        logger.addHandler(handler)
        self.logger = logger
        self.log(self.INFO, "message='----- New Logging object created. Logs will be written to the file %s. -----'" % self.log_filename)

    def log(self, log_level, log_msg):
        """ Log something

        Log log_msg with a level of log_level to self.log_filename.

        Args:
            log_level: (int | str) level of the log message
            log_msg: (str) message to log

        Returns:

        """
        if isinstance(log_level, str):
            log_level = LOG_LEVEL_DICTIONARY[log_level]
        log_msg = log_msg.replace('\n', '').replace('\r', '')  # remove carriage returns and new lines
        log_msg = re.sub(' +', ' ', log_msg)  # replace multiple spaces with one space
        self.logger.log(level=log_level, msg=log_msg)


def check_logger(logger):
    """ Check to make sure logger is valid. If not, returns a valid logger.

    Args:
        logger: (common.Util.Logging.Logging) logger

    Returns:
        logger: (common.Util.Logging.Logging) logger

    """
    if logger is None:
        log_filename = get_log_filepath('Python App')
        logger = Logging(name=__name__, log_filename=log_filename, log_level_str='INFO')
    return logger
