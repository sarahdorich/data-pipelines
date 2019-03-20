#!/usr/bin/python

"""Use a Google API to access data

Uses a Google API to access data. The following Google APIs are currently supported:
    1) Google Analytics

The init method creates a service which can then be used to connect to any Google API.
Each of the classes allows you to utilize various Google APIs.

"""

from common.util.ConfigAppUtility import ConfigAppUtility
from common.util.Logging import Logging
from common.util.OSHelpers import get_log_filepath

import argparse
import os
import pandas as pd
import logging

from googleapiclient import discovery
from googleapiclient.http import build_http


def init(api_name, api_version, api_settings_dict, discovery_filename=None):
    """ Initialize a Google API service

    Args:
        api_name: (str) name of the Google API
        api_version: (str) version of the Google API
        api_settings_dict: (dict) dictionary with Google API settings
        discovery_filename: (str) filename for pre-prepped Google API service

    Returns:
        service: ()

    """

    # Set logging levels so we don't log stuff that doesn't really matter
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)

    # Import libraries from oauth2client
    try:
        from oauth2client import client
        from oauth2client import file
        from oauth2client import tools
    except ImportError:
        raise ImportError(
            'GoogleApi requires oauth2client. Please install oauth2client and try again.')

    # Set the Google API scope
    scope = 'https://www.googleapis.com/auth/' + api_name

    # Parser command-line arguments.
    parent_parsers = [tools.argparser]
    parent_parsers.extend([])
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=parent_parsers)
    flags = parser.parse_args([])

    # Name of a file containing the OAuth 2.0 information for this
    # application, including client_id and client_secret, which are found
    # on the API Access tab on the Google APIs
    # Console <http://code.google.com/apis/console>.
    client_secrets = os.path.join(os.path.dirname(__file__),
                                  api_settings_dict['client_secrets_file'])

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(client_secrets,
                                          scope=scope,
                                          message=tools.message_if_missing(client_secrets))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage(api_name + '.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=build_http())

    if discovery_filename is None:
        # Construct a service object via the discovery service.
        # print('Constructing a service object via the discovery service.')
        service = discovery.build(api_name, api_version, http=http)
    else:
        # Construct a service object using a local discovery document file.
        with open(discovery_filename) as discovery_file:
            service = discovery.build_from_document(
                discovery_file.read(),
                base='https://www.googleapis.com/',
                http=http)
    return service


class GoogleAnalytics:
    """Interact with Google Analytics (GA)"""

    MAXRETURNEDROWS = 100000  # GA API v4 will never return more than 100,000 rows at once
    MAXMETRICS = 10  # GA API v4 will only allow you to pull 10 metrics in one report
    MAXDIMENSIONS = 9  # GA API v4 will only allow you to pull 7 dimensions in one report

    def __init__(self, account_name=None, property_name=None, profile_name=None, logging_obj=None):
        """ Create a GoogleAnalytics object

        Initializes a GoogleAnalytics object,
        creates a service to use for interacting with your Google Analytics account,
        and sets the profile ID if account_name, property_name and profile_name are provided.

        Args:
            account_name: (str) name of the GA account
            property_name: (str) name of the GA property on the GA account
            profile_name: (str) name of the GA profile on the GA property of the GA account
            logging_obj: (common.Util.Logging.Logging) initialized logging object

        Example:
            ga = GoogleAnalytics(account_name="www.usana.com",
                                 property_name="usana.com",
                                 profile_name="*www.usana.com all")
        """
        if logging_obj is None:
            log_filename = get_log_filepath('Python App')
            logging_obj = Logging(name=__name__, log_filename=log_filename, log_level_str='INFO')
        self.logging_obj = logging_obj
        config_app_util = ConfigAppUtility()
        ga_settings = config_app_util.get_settings_dict('GA')
        self.service_old = init('analytics', 'v3', ga_settings)
        self.service = init('analytics', 'v4', ga_settings)
        self.profile_id = None

        if account_name is not None and property_name is not None and profile_name is not None:
            (profile, property, account) = self.get_profile_by_name(account_name, property_name, profile_name)
            profile_id = self.get_profile_id(profile)
            self.set_profile_id(profile_id)
        else:
            log_msg = "message='The profile ID has not been set. This needs to be set prior to executing any queries.'"
            self.logging_obj.log(self.logging_obj.WARN, log_msg)

    def get_account_by_name(self, account_name):
        """ Query the Accounts collection to get an account by its name

        One user may have access to multiple Google Analytics accounts.
        This method returns an account object that corresponds to the desired account based on the account name.

        Args:
            account_name: (string) name of the account

        Returns:
            account: (dict) account from GA

        Example:
            account = ga.getAccountByName("www.usana.com")

        """
        accounts = self.service_old.management().accounts().list().execute()

        account = None
        if accounts.get('items'):
            account = next(acnt for acnt in accounts.get('items') if acnt["name"] == account_name)

        if account is None:
            log_msg = "The account named " + account_name + " does not exist!"
            print(log_msg)

        return account

    def get_webproperty_by_name(self, account_name, property_name):
        """ Query the Webproperties collection to get a property by its name

        Each Google Analytics account can have multiple properties.
        The following link explains how to set up a property on an account:
        https://support.google.com/analytics/answer/1042508?hl=en

        Args:
            account_name: (string) name of the GA account
            property_name: (string) name of the property on a GA account

        Returns:
            webproperty:
            account: (dict) account from GA

        """
        account = self.get_account_by_name(account_name)
        webproperties = self.service_old.management().webproperties().list(
            accountId=account['id']).execute()

        webproperty = None
        if webproperties.get('items'):
            webproperty = next(wp for wp in webproperties.get('items') if wp["name"] == property_name)

        if webproperty is None:
            log_msg = "The property named " + property_name + " does not exist!"
            print(log_msg)

        return (webproperty, account)

    def get_profile_by_name(self, account_name, property_name, profile_name):
        """Traverses Management API to return the profile based on names of the account, property and profile

        Query the Profile collection to get an profile by its name.
        Webproperties can have multiple views (aka profiles).
        This method returns an object for a profile based on its name.

        Args:
            account_name: (string) name of the GA account
            property_name: (string) name of the property on a GA account
            profile_name: (string) name of the profile (or view)

        Returns:
            profile:
            webproperty:
            account: (dict) account from GA

        Usage Example:


        """

        (property, account) = self.get_webproperty_by_name(account_name, property_name)

        profiles = self.service_old.management().profiles().list(
            accountId=account['id'],
            webPropertyId=property['id']).execute()

        profile = None
        if profiles.get('items'):
            try:
                profile = next(p for p in profiles.get('items') if p["name"] == profile_name)
            except Exception as ex:
                self.logging_obj.log(self.logging_obj.ERROR,
                                     """
                                     method='DataAccess.GoogleApi.GoogleAnalytics.get_profile_by_name'
                                     message='Error trying to find the desired view'
                                     profile_name='{view_name}'
                                     exception_message='{ex_msg}'""".format(ex_msg=str(ex),
                                                                            view_name=profile_name))
                raise ex

        if profile is None:
            log_msg = "The profile named " + profile_name + " does not exist!"
            print(log_msg)

        return (profile, property, account)

    def get_profile_id(self, profile):
        """ Gets the profile ID given a profile

        Args:
            profile: ()

        Returns:
            (string) Profile (or view) ID

        """
        return profile['id']

    def set_profile_id(self, profile_id):
        """ Set profile_id

        Args:
            profile_id:

        Returns:

        """
        self.profile_id = profile_id
        return None

    def query_reporting_api_v3(self,
                               start_date,
                               end_date,
                               metrics_names_cs_list,
                               dimensions_names_cs_list,
                               filters_names=None,
                               sort_names=None,
                               start_index=None):
        """ Query Google Analytics Core Reporting API (v3)

        Link to the Developer Guide for the Core Reporting API: https://developers.google.com/analytics/devguides/reporting/core/v3/coreDevguide

        The ids parameter specifies from which Google Analytics view (profile) to retrieve data.
        Complete list of query parameters (Core Reporting API - Reference Guide): https://developers.google.com/analytics/devguides/reporting/core/v3/reference
        Complete list of dimensions & metrics: https://developers.google.com/analytics/devguides/reporting/core/dimsmets

        Args:
            start_date: (str) start date
            end_date: (str) end date
            metrics_names_cs_list: (str) metric names, comma-separated list
            dimensions_names_cs_list: (str) dimension names, comma-separated list
            sort_names: (str)
            filters_names: (str)
            start_index: (str)

        Returns:
            data_dict: (dict) Data dictionary returned by GA
            data_df: (pandas.DataFrame) Data returned by GA as a dataframe

        """
        # Get response from GA
        data_dict = self.service_old.data().ga().get(
            ids='ga:' + self.profile_id,
            start_date=start_date,
            end_date=end_date,
            metrics=metrics_names_cs_list,
            dimensions=dimensions_names_cs_list,
            sort=sort_names,
            filters=filters_names,
            start_index=start_index
        ).execute()
        # Create dataframe from GA's dictionary response
        data_df = pd.DataFrame(data_dict.get('rows'))
        column_headers = []
        for header in data_dict.get('columnHeaders'):
            column_headers.append(header.get('name'))
        data_df.columns = column_headers
        return (data_dict, data_df)

    def query_reporting_api_v4(self,
                               start_date,
                               end_date,
                               metrics_names_cs_list,
                               dimensions_names_cs_list,
                               dimension_filters_dict=None):
        """ Query Google Analytics Core Reporting API (v4)

        Link to Developer Guide batchGet method: https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
        Dimensions & Metrics Explorer: https://developers.google.com/analytics/devguides/reporting/core/dimsmets
        Limits & Quotas: https://developers.google.com/analytics/devguides/reporting/core/v4/limits-quotas

        GA only allows you to pull 10 metrics and 7 dimensions at once.

        Args:
            start_date: (str) start date
            end_date: (str) end date
            metrics_names_cs_list: (str) metric names, comma-separated list
            dimensions_names_cs_list: (str) dimension names, comma-separated list
            dimension_filters_dict: (dict) dictionary of dimension filters where the keys are the dimension names to filter and the values are a list of REGEX search terms

        Returns:
            reports_dict: (dict) Response returned by GA
            data_df: (pandas.DataFrame) Data returned by GA as a dataframe
        """
        metrics_names_list = metrics_names_cs_list.split(",")
        metrics_body_list = []
        for metric_name in metrics_names_list:
            metrics_body_list.append({'expression': metric_name})

        if len(metrics_body_list) > self.MAXMETRICS:
            log_msg = """
                    method='DataAccess.GoogleApi.GoogleAnalytics.query_reporting_api_v4'
                    message='GA only allows you to pull {max_metrics} metrics at once and you are asking for {num_metrics}.'
                    """.format(max_metrics=self.MAXMETRICS,
                               num_metrics=len(metrics_body_list))
            self.logging_obj.log(self.logging_obj.WARN, log_msg)

        dimensions_names_list = dimensions_names_cs_list.split(",")
        dimensions_body_list = []
        for dimension_name in dimensions_names_list:
            dimensions_body_list.append({'name': dimension_name})

        if len(dimensions_body_list) > self.MAXDIMENSIONS:
            log_msg = """
                    method='DataAccess.GoogleApi.GoogleAnalytics.query_reporting_api_v4'
                    message='GA only allows you to pull {max_dims} dimensions at once and you are asking for {num_dims}.'
                    """.format(max_dims=self.MAXDIMENSIONS,
                               num_dims=len(dimensions_body_list))
            self.logging_obj.log(self.logging_obj.WARN, log_msg)

        dimension_filters_body_list = []
        if dimension_filters_dict is not None:
            for dimension_filter_key, dimension_filter_value in dimension_filters_dict.items():
                dimension_filters_body_list.append({'dimensionName': dimension_filter_key,
                                                    'operator': 'REGEXP',
                                                    'expressions': dimension_filter_value})

        page_token_i = '0'
        is_max_returned_rows = True
        reports_dict = dict()
        data_df = pd.DataFrame()
        i = 0
        while is_max_returned_rows:
            # GA API v4 outputs a list of dictionaries - one dict for each report.
            # This method only returns one report.
            reports_dict_i = self.service.reports().batchGet(
                body={
                    'reportRequests': [
                        {
                            'viewId': self.profile_id,
                            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                            'metrics': metrics_body_list,
                            'dimensions': dimensions_body_list,
                            'pageSize': self.MAXRETURNEDROWS,
                            'pageToken': page_token_i,
                            'dimensionFilterClauses': [{
                                'filters': dimension_filters_body_list
                            }]
                        }]
                }
            ).execute()

            # Log possible issues with the data set pulled from GA
            self.check_response_data_quality(reports_dict_i)

            # Turn the dict response to a pandas DataFrame
            data_df_i = self.api_response_to_dataframe_v4(reports_dict_i)

            reports_dict["dataset{i}".format(i=i)] = reports_dict_i
            data_df = data_df.append(data_df_i, ignore_index=True)

            if data_df_i.shape[0] == self.MAXRETURNEDROWS:
                log_msg = """
                        method='DataAccess.GoogleApi.GoogleAnalytics.query_reporting_api_v4'
                        message='The number of rows in the dataframe returned by Google Analytics API v4 is the same size as the max that the API allows. The method will loop until we get all the data.'
                        i='{i}'
                        page_token_i='{page_token_i}'
                        """.format(i=i,
                                   page_token_i=page_token_i)
                self.logging_obj.log(self.logging_obj.WARN, log_msg)
                page_token_i = self.get_next_page_token(reports_dict_i)
            else:
                is_max_returned_rows = False
            i = i + 1

        return (reports_dict, data_df)

    def api_response_to_dataframe_v4(self, response):
        """ Convert a GA API v4 response to a dataframe

        Args:
            response: (dict) response from GA

        Returns:
            df: (pandas.DataFrame) response from GA as a dataframe
        """
        list = []
        # get report data
        for report in response.get('reports', []):
            # set column headers
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])

            for row in rows:
                # create dict for each row
                dict = {}
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                # fill dict with dimension header (key) and dimension value (value)
                for header, dimension in zip(dimension_headers, dimensions):
                    dict[header] = dimension

                # fill dict with metric header (key) and metric value (value)
                for i, values in enumerate(dateRangeValues):
                    for metric, value in zip(metric_headers, values.get('values')):
                        # set int as int, float a float
                        if ',' in value or '.' in value:
                            dict[metric.get('name')] = float(value)
                        else:
                            dict[metric.get('name')] = int(value)

                list.append(dict)

            df = pd.DataFrame(list)
            return df

    def get_next_page_token(self, response):
        next_page_token = response['reports'][0]['nextPageToken']
        return next_page_token

    def check_response_data_quality(self, api_response):
        """Checks the data quality of the response the GA API returned

        Logs the following data quality issues:
        1) Golden data
        2) Sampling - based on https://developers.google.com/analytics/devguides/reporting/core/v4/basics#sampling

        TODO: log these things in a table in the database as it's important to know (especially sampling)

        Args:
            api_response (dict): response from GA API

        Returns:

        """
        # Check to see if the data is golden (if it is not, this means it could change over time)
        try:
            is_data_golden = api_response['reports'][0]['data']['isDataGolden']
        except:
            log_msg = """
                        method='DataAccess.GoogleApi.GoogleAnalytics.check_response_data_quality'
                        message='The isDataGolden key does not exist.'
                        """
            self.logging_obj.log(self.logging_obj.DEBUG, log_msg)
        else:
            if not is_data_golden:
                log_msg = """
                            method='DataAccess.GoogleApi.GoogleAnalytics.check_response_data_quality'
                            message='This data set is not golden (data is golden when the exact same request will not 
                                produce any new results if asked at a later point in time).'
                            """
                self.logging_obj.log(self.logging_obj.WARN, log_msg)
        # Check to see if the data set is sampled
        try:
            samples_read_counts = api_response['reports'][0]['data']['samplesReadCounts']
            sampling_space_sizes = api_response['reports'][0]['data']['samplingSpaceSizes']
        except:
            log_msg = """
                        method='DataAccess.GoogleApi.GoogleAnalytics.check_response_data_quality'
                        message='This data set is not sampled! Yay!!! :)'
                        """
            self.logging_obj.log(self.logging_obj.DEBUG, log_msg)
        else:
            log_msg = """
                        method='DataAccess.GoogleApi.GoogleAnalytics.check_response_data_quality'
                        message='This data set IS sampled!!! Do not trust for analysis!'
                        samples_read_counts='{samples_read_counts}'
                        sampling_space_sizes='{sampling_space_sizes}'
                        """.format(samples_read_counts=samples_read_counts,
                                   sampling_space_sizes=sampling_space_sizes)
            self.logging_obj.log(self.logging_obj.WARN, log_msg)





