#!/usr/bin/python

"""Helpers for renaming GA columns

"""

COLUMNMAPPER = {'ga:date': 'DateMst',
                'ga:country': 'Country',
                'ga:region': 'Region',
                'ga:source': 'Source',
                'ga:medium': 'Medium',
                'ga:deviceCategory': 'DeviceCategory',
                'ga:goalCompletionLocation': 'GoalCompletionLocation',
                'ga:pagePath': 'PagePath',
                'ga:hostname': 'Hostname',
                'ga:landingPagePath': 'LandingPagePath',
                'ga:exitPagePath': 'ExitPagePath',
                'ga:previousPagePath':'PreviousPagePath',
                'ga:sourceMedium':'SourceMedium',
                'ga:pageDepth':'PageDepth',
                'ga:users': 'Users',
                'ga:newUsers': 'NewUsers',
                'ga:sessionsPerUser': 'SessionsPerUser',
                'ga:sessions': 'Sessions',
                'ga:avgSessionDuration': 'AvgSessionDurationSeconds',
                'ga:sessionDuration': 'SessionDurationSeconds',
                'ga:bounceRate': 'BounceRate',
                'ga:bounces': 'Bounces',
                'ga:pageviewsPerSession': 'PageviewsPerSession',
                'ga:uniquePageviews': 'UniquePageviews',
                'ga:avgPageLoadTime': 'AvgPageLoadTimeSeconds',
                'ga:pageviews': 'Pageviews',
                'ga:goalStartsAll': 'GoalStartsAll',
                'ga:goalCompletionsAll': 'GoalCompletionsAll',
                'ga:goalValueAll': 'GoalValueAll',
                'ga:goalValuePerSession': 'GoalValuePerSession',
                'ga:goalConversionRateAll': 'GoalConversionRateAll',
                'ga:goalAbandonsAll': 'GoalAbandonsAll',
                'ga:goalAbandonRateAll': 'GoalAbandonRateAll',
                'ga:timeOnPage': 'TimeOnPageSeconds',
                'ga:avgTimeOnPage': 'AvgTimeOnPageSeconds',
                'ga:entrances': 'Entrances',
                'ga:exitRate': 'ExitRate',
                'ga:pageValue': 'PageValue',
                'ga:exits': 'Exits',
                'ga:entranceRate':'EntranceRate',
                'sum(ga:pageviews)': 'Pageviews',
                'sum(ga:uniquePageviews)': 'UniquePageviews'}


def update_column_names(ga_column_list):
    """Update column names given a list of columns

    Args:
        ga_column_list (list of strings): list of GA column names

    Returns:
        column_list (list of strings): list of column names based on COLUMNMAPPER

    """
    column_list = []
    for column_name in ga_column_list:
        try:
            updated_column_name = COLUMNMAPPER[column_name]
        except Exception:
            print("message='{col_name} is not a valid GA column'".format(col_name=column_name))
            updated_column_name = column_name

        column_list.append(updated_column_name)
    return column_list


def update_spark_df_column_names(spark_df):
    """Update the column names of a Spark DataFrame

    Args:
        spark_df:

    Returns:

    """
    ga_column_list = spark_df.columns
    for column_name in ga_column_list:
        try:
            updated_column_name = COLUMNMAPPER[column_name]
        except Exception:
            print("message='{col_name} is not a valid GA column'".format(col_name=column_name))
            updated_column_name = column_name

        spark_df = spark_df.withColumnRenamed(column_name, updated_column_name)
    return spark_df
