#!/usr/bin/python

"""Tools for interacting with Amazon Web Services

"""

S3KEYROOT = "google_analytics"


def create_s3_bucket_key(data_description_for_key,
                         app_config_for_key,
                         partition_parameter_for_key,
                         data_parameter_config_for_key,
                         object_extension=None):
    """ Create key for location of data in a S3 bucket

    Args:
        data_description_for_key (str): description of data set
        app_config_for_key (str): configuration of GA application
        partition_parameter_for_key (str): parameter to partition data by
        data_parameter_config_for_key (str): data parameter configuration for further partitioning/organizing of data
        object_extension (str): extension of the object, e.g. a file extension

    Returns:
        full_key (str): full key to data in a S3 bucket

    Example:
        my_full_key = create_s3_bucket_key(data_description_for_key='daily_regional_summary',
                                           app_config_for_key='View=1',
                                           data_parameter_config_for_key='01-02-2019')

    """
    if object_extension is not None:
        full_key = S3KEYROOT + "/" \
                   + data_description_for_key + "/" \
                   + app_config_for_key + "/" \
                   + partition_parameter_for_key + "/" \
                   + data_parameter_config_for_key + "." \
                   + object_extension
    else:
        full_key = S3KEYROOT + "/" \
                   + data_description_for_key + "/" \
                   + app_config_for_key + "/" \
                   + partition_parameter_for_key + "/" \
                   + data_parameter_config_for_key

    return full_key

