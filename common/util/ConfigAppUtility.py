#!/usr/bin/python

"""Contains a class to use for configuring your Python application"""

import configparser

APP_CONFIG_FILE = "config/app_config.ini"


class ConfigAppUtility():
    """Utility to use for configuring your Python application"""

    def __init__(self, app_config_file_override=None):
        """ Create a ConfigAppUtility object

        Initializes a ConfigParser object and opens the APP_CONFIG_FILE.

        Args:

        Returns:

        Example:
            config_app_util = ConfigAppUtility()
        """
        config = configparser.ConfigParser()
        if app_config_file_override is not None:
            config.read(app_config_file_override)
        else:
            config.read(APP_CONFIG_FILE)
        self.config = config

    def config_section_map(self, section):
        """Create a dictionary of key/value pairs for a section in APP_CONFIG_FILE

        Args:
            section: (string) Name of section in APP_CONFIG_FILE

        Returns:
            dict1: (dict) Key/value pairs for settings in the input section from APP_CONFIG_FILE

        Example:
            config_app_util = ConfigAppUtility()
            settings_dict = config_app_util.config_section_map(section_name)

        """
        dict1 = {}
        options = self.config.options(section)
        for option in options:
            try:
                dict1[option] = self.config.get(section, option)
                if dict1[option] == -1:
                    print("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1

    def get_settings_dict(self, section_name):
        """ Get a dict of settings for a section in APP_CONFIG_FILE

        Args:
            section_name: (string) Name of the section in APP_CONFIG_FILE

        Returns:
            settings_dict: (dict) Key/value pairs for settings in the input section from APP_CONFIG_FILE

        Example:
            config_app_util = ConfigAppUtility()
            ga_settings = config_app_util.get_settings_dict('GA')
            client_secrets_file = ga_settings['client_secrets_file']

        """
        settings_dict = self.config_section_map(section_name)
        return settings_dict

    def get_list_of_values(self, dict_value, sep=","):
        dict_value_list = dict_value.split(sep)
        return dict_value_list



