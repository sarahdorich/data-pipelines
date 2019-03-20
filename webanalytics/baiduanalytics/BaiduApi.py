#!/usr/bin/python

"""Use the Tongji API to access data from Baidu Analytics

Uses the Tongji API to access data from Baidu Analytics. The following APIs are currently supported:
    1) https://api.baidu.com/json/tongji/v1/ReportService/getData

"""

from usanapy.Util.Logging import check_logger
from usanapy.Util.ListMethods import is_in

import inspect
import json
import urllib.request
import urllib.parse
import pandas as pd
import os, ssl
import numpy as np


class BaiduAnalytics:
    """Interact with Baidu Analytics (BA)"""

    BASE_URL = "https://api.baidu.com/json/tongji/v1/ReportService/getData"

    def __init__(self, site_id, username, password, token, logger=None):
        """ Create a DataAccess.BaiduApi.BaiduAnalytics object

        Args:
            site_id (str): ID of the site
                The site_id can be found in Baidu Analytics user interface (https://tongji.baidu.com/web/homepage/index)
                Then go to Management > get code (for whichever site you want to track).
                Then in the URL of the page that pops up in a new window, there will be ?siteId=xxxxxxx appended to it.
                Everything after ?siteId= is your site_id.
            username (str): Baidu Analytics username
            password (str): Baidu Analytics password
            token (str): token from Baidu Analytics
                To get your token, use an admin account in Baidu Analytics.
                In Baidu Analytics (translated page), go to Management > Other settings > Data export service.
                Accept and open it and the token will display on the screen.
            logger (usanapy.Util.Logging.Logging): logger
        """
        self.site_id = site_id
        self.username = username
        self.password = password
        self.token = token
        self.logger = check_logger(logger)

    def get_report_api_response(self, report_method, start_date, end_date, metrics_cs_list):
        """ Get a report from Baidu Analytics

        Args:
            method (str): usually corresponds to the report to be queried
            start_date (str): date of the format yyyymmdd
            end_date (str): date of the format yyyymmdd
            metrics_cs_list (str): metric names, comma-separated list

        Returns:
            result (dict): result from Baidu Analytics API call

        Example:
            from DataAccess.BaiduApi import BaiduAnalytics
            baidu_api = BaiduAnalytics(site_id="12615124", username="un", password="pwd", token="32b43nk")
            body = baidu_api.create_request_body(method="visit/district/a",
                                                 start_date="2019-01-01",
                                                 end_date="2019-01-07",
                                                 metrics_cs_list="pv_count,visitor_count,avg_visit_time")

        """
        # Begin insecure statement
        if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
                getattr(ssl, '_create_unverified_context', None)):
            ssl._create_default_https_context = ssl._create_unverified_context
        # End of insecure statement
        body = {"header": {"account_type": 1,
                           "password": self.password,
                           "token": self.token,
                           "username": self.username},
                "body": {"siteId": self.site_id,
                         "method": report_method,
                         "start_date": start_date,
                         "end_date": end_date,
                         "metrics": metrics_cs_list}}
        data = bytes(json.dumps(body), 'utf8')
        req = urllib.request.Request(self.BASE_URL, data)
        response = urllib.request.urlopen(req)
        the_page = response.read()
        result = json.loads(the_page.decode("utf-8"))
        if result['header']['status'] != 0:
            log_msg = """method=DataAccess.BaiduApi.BaiduAnalytics.{method} 
                         message='Error getting data from Baidu Analytics' 
                         header_msg='{header_msg}'""".format(method=inspect.stack()[0][3],
                                                             header_msg=result['header']['failures'][0])
            self.logger.log(self.logger.ERROR, log_msg)
            raise Exception(log_msg)
        return result

    def get_response_fields(self, api_response):
        response_fields = api_response["body"]["data"][0]["result"]["fields"]
        return response_fields

    def api_response_to_pandas(self, api_response):
        """ Convert an API response to a Pandas DataFrame

        Args:
            api_response (dict):

        Returns:
            response_df (pandas.DataFrame):

        """
        if api_response["body"]["data"][0]["result"]["total"] > 0:
            dimension_fields = list(api_response["body"]["data"][0]["result"]["items"][0][0][0].keys())
            response_dimensions = api_response["body"]["data"][0]["result"]["items"][0]
            response_metrics = api_response["body"]["data"][0]["result"]["items"][1]
            num_metrics = len(response_metrics[0])
            response_fields = self.get_response_fields(api_response)
            metric_fields = response_fields[-num_metrics:]
            dimensions_list = []
            for field in dimension_fields:
                field_iter_list = []
                for item in response_dimensions:
                    field_iter_list.append(item[0][field])
                dimensions_list.append(field_iter_list)
            iter = 0
            response_list = []
            for item in response_metrics:
                item_json = {}
                dim_items_iter = 0
                for dim_items in dimensions_list:
                    item_json[dimension_fields[dim_items_iter]] = dim_items[iter]
                    dim_items_iter = dim_items_iter + 1
                for metric_iter in range(num_metrics):
                    item_json[metric_fields[metric_iter]] = item[metric_iter]
                iter = iter + 1
                response_list.append(item_json)
            response_df = pd.DataFrame(response_list)
            response_df = self.clean_pandas(response_df)
        else:
            log_msg = """method=DataAccess.BaiduApi.BaiduAnalytics.{method} 
                         message='Zero records of data returned from Baidu Analytics' 
                         """.format(method=inspect.stack()[0][3])
            self.logger.log(self.logger.WARN, log_msg)
            response_df = None
        return response_df

    def clean_pandas(self, response_df):
        if is_in('avg_visit_time', response_df.columns.values):
            response_df['avg_visit_time'] = response_df['avg_visit_time'].replace('--', np.nan)
        if is_in('bounce_ratio', response_df.columns.values):
            response_df['bounce_ratio'] = response_df['bounce_ratio'].replace('--', np.nan)
        return response_df

