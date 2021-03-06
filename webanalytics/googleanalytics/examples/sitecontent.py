#!/usr/bin/python

"""Move GA site content data

"""

from webanalytics.googleanalytics.examples.ietl import IEtl
from webanalytics.googleanalytics.examples.tools.FeatureExtractors import get_page_path_levels, \
    extract_page_path_level_n, extract_source_medium
from webanalytics.googleanalytics.examples.tools.GoogleAnalyticsColumnRenamer import update_spark_df_column_names, \
    update_column_names

import inspect
import pandas as pd
import numpy as np


DESCRIPTIONFORS3KEY = "daily_site_content"


class DailySiteContent(IEtl):
    """Move DailySiteContent data from GA to AWS"""

    def __init__(self,
                 ga_api_obj,
                 s3_bucket,
                 ga_view_id,
                 start_date,
                 end_date,
                 logger):
        # Initialize class variables
        super().__init__(data_source=ga_api_obj,
                         data_sink=s3_bucket,
                         ga_view_id=ga_view_id,
                         start_date=start_date,
                         end_date=end_date,
                         logger=logger)
        # Set values for S3 bucket key
        self.s3_bucket_key = self.get_s3_key(DESCRIPTIONFORS3KEY)

    def extract(self):
        metrics_names_cs_list1 = 'ga:pageviews,ga:uniquePageviews,ga:timeOnPage,ga:avgTimeOnPage,ga:entrances,ga:bounceRate,ga:exitRate,ga:pageValue'
        dimensions_names_cs_list = 'ga:date,ga:sourceMedium,ga:country,ga:landingPagePath,ga:hostname,ga:pagePath,ga:previousPagePath,ga:pageDepth,ga:exitPagePath'
        dimension_filters_dict = None

        log_msg = """
                    method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContent.{method}'
                    message='Getting 1st set of site content summary data'
                    metrics_names_cs_list='{metrics_names}'
                    dimensions_names_cs_list='{dimensions_names}'
                    start_date='{s_date}'
                    end_date='{e_date}'
                    """.format(method=inspect.stack()[0][3],
                               metrics_names=metrics_names_cs_list1,
                               dimensions_names=dimensions_names_cs_list,
                               s_date=self.start_date,
                               e_date=self.end_date)
        self.logger.log(self.logger.INFO, log_msg)
        (reports_dict1, data_df1) = self.data_source.query_reporting_api_v4(self.start_date, self.end_date,
                                                                            metrics_names_cs_list1,
                                                                            dimensions_names_cs_list,
                                                                            dimension_filters_dict)
        metrics_names_cs_list2 = 'ga:entranceRate,ga:pageviewsPerSession,ga:exits,ga:avgSessionDuration,ga:sessions'
        log_msg = """
                    method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContent.{method}'
                    message='Getting 2nd set of site content summary data'
                    metrics_names_cs_list='{metrics_names}'
                    dimensions_names_cs_list='{dimensions_names}'
                    start_date='{s_date}'
                    end_date='{e_date}'
                    """.format(method=inspect.stack()[0][3],
                               metrics_names=metrics_names_cs_list2,
                               dimensions_names=dimensions_names_cs_list,
                               s_date=self.start_date,
                               e_date=self.end_date)
        self.logger.log(self.logger.INFO, log_msg)
        (reports_dict2, data_df2) = self.data_source.query_reporting_api_v4(self.start_date, self.end_date,
                                                                            metrics_names_cs_list2,
                                                                            dimensions_names_cs_list,
                                                                            dimension_filters_dict)
        if data_df1.empty or data_df2.empty:
            log_msg = """
                        method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContent.{method}'
                        message='Not merging daily site content data because at least one of the responses is empty'
                        """.format(method=inspect.stack()[0][3])
            self.logger.log(self.logger.WARN, log_msg)
            data_df = data_df1
        else:
            data_df = pd.merge(data_df1, data_df2,
                               on=['ga:date',
                                   'ga:sourceMedium',
                                   'ga:country',
                                   'ga:landingPagePath',
                                   'ga:hostname',
                                   'ga:pagePath',
                                   'ga:previousPagePath',
                                   'ga:pageDepth',
                                   'ga:exitPagePath'],
                               how='outer')
        return data_df

    def transform(self, response):
        if response.empty:
            log_msg = """
                        method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContent.{method}'
                        message='Not transforming daily site content data because response is empty'
                        """.format(method=inspect.stack()[0][3])
            self.logger.log(self.logger.WARN, log_msg)
            transformed_response = None
        else:
            # Update column names
            new_column_names = update_column_names(response.columns.values)
            response.columns = new_column_names

            # Extract page path levels and source/medium
            source_medium_series = response['SourceMedium']
            page_path_series = response['PagePath']
            landing_page_path_series = response['LandingPagePath']
            exit_page_path_series = response['ExitPagePath']
            previous_page_path_series = response['PreviousPagePath']

            source = []
            medium = []
            page_path_level1 = []
            page_path_level2 = []
            landing_page_path_level1 = []
            landing_page_path_level2 = []
            exit_page_path_level1 = []
            exit_page_path_level2 = []
            previous_page_path_level1 = []
            previous_page_path_level2 = []

            for iter in range(len(source_medium_series)):

                source_iter, medium_iter = extract_source_medium(source_medium_series[iter])

                page_path_levels_list = get_page_path_levels(page_path_series[iter])
                page_path_level1_val = extract_page_path_level_n(page_path_levels_list, 1)
                page_path_level2_val = extract_page_path_level_n(page_path_levels_list, 2)

                landing_levels_list_iter = get_page_path_levels(landing_page_path_series[iter])
                landing_page_path_level1_iter = extract_page_path_level_n(landing_levels_list_iter, 1)
                landing_page_path_level2_iter = extract_page_path_level_n(landing_levels_list_iter, 2)

                exit_levels_list_iter = get_page_path_levels(exit_page_path_series[iter])
                exit_page_path_level1_iter = extract_page_path_level_n(exit_levels_list_iter, 1)
                exit_page_path_level2_iter = extract_page_path_level_n(exit_levels_list_iter, 2)

                previous_levels_list_iter = get_page_path_levels(previous_page_path_series[iter])
                previous_page_path_level1_iter = extract_page_path_level_n(previous_levels_list_iter, 1)
                previous_page_path_level2_iter = extract_page_path_level_n(previous_levels_list_iter, 2)

                source.append(source_iter)
                medium.append(medium_iter)

                page_path_level1.append(page_path_level1_val)
                page_path_level2.append(page_path_level2_val)

                landing_page_path_level1.append(landing_page_path_level1_iter)
                landing_page_path_level2.append(landing_page_path_level2_iter)

                exit_page_path_level1.append(exit_page_path_level1_iter)
                exit_page_path_level2.append(exit_page_path_level2_iter)

                previous_page_path_level1.append(previous_page_path_level1_iter)
                previous_page_path_level2.append(previous_page_path_level2_iter)

            response = response.assign(Source=source,
                                       Medium=medium,
                                       PagePathLevel1=page_path_level1,
                                       PagePathLevel2=page_path_level2,
                                       LandingPagePathLevel1=landing_page_path_level1,
                                       LandingPagePathLevel2=landing_page_path_level2,
                                       ExitPagePathLevel1=exit_page_path_level1,
                                       ExitPagePathLevel2=exit_page_path_level2,
                                       PreviousPagePathLevel1=previous_page_path_level1,
                                       PreviousPagePathLevel2=previous_page_path_level2)

            # Prepare a DataFrame for loading into a staging table
            num_rows = response.shape[0]
            data_tuples = list(zip([self.ga_view_id] * num_rows,
                                   response['DateMst'],
                                   response['SourceMedium'],
                                   response['Country'],
                                   response['LandingPagePath'],
                                   response['Hostname'],
                                   response['PagePath'],
                                   response['PreviousPagePath'],
                                   response['PageDepth'],
                                   response['ExitPagePath'],
                                   response['Pageviews'],
                                   response['UniquePageviews'],
                                   response['TimeOnPageSeconds'],
                                   response['AvgTimeOnPageSeconds'],
                                   response['Entrances'],
                                   response['BounceRate'],
                                   response['ExitRate'],
                                   response['PageValue'],
                                   response['EntranceRate'],
                                   response['PageviewsPerSession'],
                                   response['Exits'],
                                   response['AvgSessionDurationSeconds'],
                                   response['Sessions'],
                                   response['Source'],
                                   response['Medium'],
                                   response['PagePathLevel1'],
                                   response['PagePathLevel2'],
                                   response['LandingPagePathLevel1'],
                                   response['LandingPagePathLevel2'],
                                   response['ExitPagePathLevel1'],
                                   response['ExitPagePathLevel2'],
                                   response['PreviousPagePathLevel1'],
                                   response['PreviousPagePathLevel2']
                                   ))
            transformed_response_column_names = np.insert(response.columns.values, 0, 'ViewId')
            transformed_response = pd.DataFrame(data_tuples, columns=transformed_response_column_names)

        return transformed_response

    def load(self, transformed_response):
        if transformed_response is not None:
            log_msg = """
                        method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContent.{method}'
                        message='Uploading daily site content summary data'
                        key={key}
                        """.format(method=inspect.stack()[0][3],
                                   key=self.s3_bucket_key)
            self.logger.log(self.logger.INFO, log_msg)
            self.data_sink.pandas_to_s3_parquet(transformed_response, self.s3_bucket_key)
            log_msg = """
                        method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContent.{method}'
                        message='Successfully uploaded daily site content summary data'
                        key={key}
                        """.format(method=inspect.stack()[0][3],
                                   key=self.s3_bucket_key)
            self.logger.log(self.logger.INFO, log_msg)
        else:
            log_msg = """
                        method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContent.{method}'
                        message='Not uploading daily site content data because response is empty'
                        """.format(method=inspect.stack()[0][3])
            self.logger.log(self.logger.WARN, log_msg)


class DailySiteContentPagePathSummary(IEtl):

    def __init__(self,
                 aws_data_accessor,
                 db,
                 ga_view_id,
                 start_date,
                 end_date,
                 logger):
        # Initialize class variables
        super().__init__(data_source=aws_data_accessor,
                         data_sink=db,
                         ga_view_id=ga_view_id,
                         start_date=start_date,
                         end_date=end_date,
                         logger=logger)
        # Set values for S3 bucket key
        self.s3_bucket_key = self.get_s3_key(DESCRIPTIONFORS3KEY)

    def extract(self):
        df = self.data_source.get_spark_df(self.s3_bucket_key)
        return df

    def transform(self, response):
        # Summarize data
        group_by = response.groupBy("ga:date", "ga:country", "ga:pagePath")
        summary_spark_df = group_by.sum("ga:pageviews", "ga:uniquePageviews")
        summary_spark_df = update_spark_df_column_names(summary_spark_df)
        # TODO: need to figure out a different way than .toPandas() to convert to a Pandas df using latest
        #  version of pyspark
        summary_pandas_df = summary_spark_df.toPandas()
        # summary_df.show() #  for debugging
        # Prepare DataFrame for loading into a staging table
        num_rows = summary_pandas_df.shape[0]
        data_tuples = list(zip([self.ga_view_id] * num_rows,
                               summary_pandas_df['DateMst'],
                               summary_pandas_df['Country'],
                               summary_pandas_df['PagePath'],
                               summary_pandas_df['Pageviews'],
                               summary_pandas_df['UniquePageviews']))
        transformed_response = pd.DataFrame(data_tuples)
        return transformed_response

    def load(self, transformed_response):
        if transformed_response is not None:
            log_msg = """
                        method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContentPagePathSummary.{method}'
                        message='Uploading daily site content page path summary data'
                        """.format(method=inspect.stack()[0][3])
            self.logger.log(self.logger.INFO, log_msg)
            # Load staging_table_df into a staging table
            staging_table_name = "StagingDailySiteContentPagePathSummary"
            self.data_sink.truncate_table(staging_table_name)
            self.data_sink.save_dataframe_to_table(transformed_response, staging_table_name)

            log_msg = """
                        method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContentPagePathSummary.{method}'
                        message='Successfully uploaded daily site content page path summary data'
                        """.format(method=inspect.stack()[0][3])
            self.logger.log(self.logger.INFO, log_msg)
        else:
            log_msg = """
                        method='webanalytics.googleanalytics.examples.sitecontent.DailySiteContentPagePathSummary.{method}'
                        message='Not uploading daily site content page path summary data because response is None'
                        """.format(method=inspect.stack()[0][3])
            self.logger.log(self.logger.WARN, log_msg)
