""" Tasks for the data pipeline

"""

from webanalytics.googleanalytics.examples.sitecontent import DailySiteContent


def execute_dailysitecontent_export(ga_api_obj, s3_bucket, ga_view_id, start_date, end_date, logger):
    export = DailySiteContent(ga_api_obj, s3_bucket, ga_view_id, start_date, end_date, logger)
    response = export.extract()
    transformed_response = export.transform(response)
    export.load(transformed_response)


