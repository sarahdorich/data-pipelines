#!/usr/bin/python

""" Main script for a Google Analytics data pipeline
"""


from common.util.ConfigAppUtility import ConfigAppUtility
from common.util.OSHelpers import get_log_filepath
from common.util.Logging import Logging
from webanalytics.googleanalytics.GoogleApi import GoogleAnalytics as GoogleAnalyticsApi
#from common.dataaccess.SqlDatabase import SqlDatabase
from common.dataaccess.AmazonWebServicesApi import S3Bucket
from common.util.DateTimeMethods import get_curr_date_str, add_days_to_date_str, is_date1_lteq_date2
from webanalytics.googleanalytics.examples.tasks import execute_dailysitecontent_export

import sys, getopt
import pandas as pd


def main(argv):
    """ Main method

    Args:
        argv:
            -s: start date
            -e: end date

    Returns:

    Example:
        python __main__.py -s 2019-01-01 -e 2019-03-01

    """

    # Check to see if start and end dates were supplied
    opts, args = getopt.getopt(argv, "s:e:", ["start-date=", "end-date="])

    if len(opts) == 2:
        start_date = opts[0][1]
        end_date = opts[1][1]
    else:
        start_date = add_days_to_date_str(get_curr_date_str(), -7)  # Go back 1 week to ensure data is up-to-date
        end_date = add_days_to_date_str(get_curr_date_str(), -1)  # No reason to gather today's data as we will get a full load of it in tomorrow's load

    # Get configuration settings
    config_app_util = ConfigAppUtility("../../../config/app_config.ini")
    log_settings_dict = config_app_util.config_section_map('Logging')
    database_settings_dict = config_app_util.config_section_map('SqlDatabase')
    gaapi_settings_dict = config_app_util.config_section_map('GA')
    aws_settings_dict = config_app_util.config_section_map('AWS')

    # Initialize logging object
    log_filename = get_log_filepath(log_settings_dict['log_filename'])
    logger = Logging(name=__name__, log_filename=log_filename, log_level_str=log_settings_dict['logging_level'])
    logger.log(logger.INFO, """ ****************************
                                message='Running {filename}'
                                start_date='{start_date}'
                                end_date='{end_date}'
                                ****************************""".format(filename=__file__,
                                                                       start_date=start_date,
                                                                       end_date=end_date))

    try:

        # Initialize S3 bucket
        s3_bucket_obj = S3Bucket(bucket_name=aws_settings_dict['s3_bucket'],
                                 creds_profile_name=aws_settings_dict['creds_profile_name'],
                                 region_name=aws_settings_dict['region_name'],
                                 logging_obj=logger)

        # Initialize accessor to the SQL Server database
        #ga_db = SqlDatabase(server=database_settings_dict['server_name'],
         #                   database=database_settings_dict['database_name'],
          #                  driver=database_settings_dict['driver'],
           #                 port=database_settings_dict['port_number'],
            #                username=database_settings_dict['username'],
             #               password=database_settings_dict['password'],
              #              logging_obj=logger)

        # Get active GA views
        active_views_list = [{'ViewId': 1, 'AccountName': 'www.usana.com', 'PropertyName': 'New Shop Testing', 'ViewName': 'All Web Site Data'},
                             {'ViewId': 2, 'AccountName': 'Mathemagic', 'PropertyName': 'mathemagic.xyz', 'ViewName': '*mathemagic.xyz all'}]
        active_views = pd.DataFrame(active_views_list)

        # Loop through all GA views we want to use in our data pipeline
        for index, view in active_views.iterrows():

            # Google Analytics
            ga_db_view_id = view['ViewId']
            account_name = view['AccountName']
            property_name = view['PropertyName']
            profile_name = view['ViewName']

            log_msg = """
                ****************************
                message='Running the data pipeline for a GA view'
                ga_db_view_id='{view_id}'
                account_name='{account_name}'
                property_name='{property_name}'
                profile_name='{profile_name}'
                ****************************
                """.format(view_id=ga_db_view_id,
                           account_name=account_name,
                           property_name=property_name,
                           profile_name=profile_name)
            logger.log(logger.INFO, log_msg)

            # Initialize GA API access
            ga_api = GoogleAnalyticsApi(account_name=account_name,
                                        property_name=property_name,
                                        profile_name=profile_name,
                                        ga_settings=gaapi_settings_dict,
                                        logging_obj=logger)

            # Loop by date and run the data pipeline
            start_date_i = start_date
            end_date_i = add_days_to_date_str(start_date_i, 0)

            while is_date1_lteq_date2(start_date_i, end_date):
                log_msg = """
                    ****************************
                    message='Running data pipeline for the date range {start_date_i} - {end_date_i}'
                    ****************************
                    """.format(start_date_i=start_date_i,
                               end_date_i=end_date_i)
                logger.log(logger.INFO, log_msg)

                execute_dailysitecontent_export(ga_api, s3_bucket_obj, ga_db_view_id, start_date_i, end_date_i, logger)

                # Increment start and end dates
                start_date_i = add_days_to_date_str(end_date_i, 1)
                end_date_i = add_days_to_date_str(start_date_i, 0)

    except Exception as ex:
        logger.log(logger.ERROR, "message='Error running {filename}' exception_message='{ex_msg}'".format(filename=__file__,
                                                                                                          ex_msg=str(ex)))
        raise ex

    else:
        logger.log(logger.INFO, "message='Successfully ran {filename}'".format(filename=__file__))


if __name__ == "__main__":
    main(sys.argv[1:])

