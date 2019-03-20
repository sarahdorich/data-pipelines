#!/usr/bin/python

"""Interface for extracting, transforming and loading data

"""

from common.util.DateTimeMethods import get_month_of_date, get_year_of_date
from common.dataaccess.AmazonWebServicesTools import create_s3_bucket_key

from abc import ABCMeta, abstractmethod


class IEtl(object, metaclass=ABCMeta):

    @abstractmethod
    def __init__(self,
                 data_source,
                 data_sink,
                 ga_view_id,
                 start_date,
                 end_date,
                 logger):
        """ Initialize an ETL object

        Initialize an object used for extracting, transforming and loading data.

        Args:
            data_source: object that allows interaction with the data source
            data_sink: object that allows interaction with the data sink (i.e. medium capable of receiving data)
            ga_view_id (): ID of the view (or profile)
            start_date (str): start date of the data pull
            end_date (str): end date of the data pull
            logger (common.util.Logging.Logging): logger object
        """
        self.data_source = data_source
        self.data_sink = data_sink
        self.ga_view_id = ga_view_id
        self.start_date = start_date
        self.end_date = end_date
        self.logger = logger

    @abstractmethod
    def extract(self):
        return None

    @abstractmethod
    def transform(self, response):
        return None

    @abstractmethod
    def load(self, transformed_response):
        pass

    def get_s3_key(self, data_description_for_key):
        ga_config_for_key = "ViewId={view_id}".format(view_id=self.ga_view_id)
        if self.start_date == self.end_date:
            data_parameter_config_for_key = self.start_date
        else:
            data_parameter_config_for_key = self.start_date + "_" + self.end_date
        # The partition parameter for site content data is the month year of the start date
        partition_parameter_for_key = str(get_year_of_date(self.start_date)) + '-' + str(
            get_month_of_date(self.start_date)) + '-01'
        s3_bucket_key = create_s3_bucket_key(data_description_for_key,
                                             ga_config_for_key,
                                             partition_parameter_for_key,
                                             data_parameter_config_for_key,
                                             object_extension='parquet')
        return s3_bucket_key




