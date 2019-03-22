#!/usr/bin/python

"""Access data in Amazon Web Services (AWS)

"""

from common.util.Logging import Logging
from common.util.OSHelpers import get_log_filepath
from common.util.ListMethods import is_in
from common.util.DateTimeMethods import get_curr_datetime_str

import boto3
import logging
import io
import pandas as pd
import gzip
from boto3.s3.transfer import TransferConfig
import re


class AwsInitializer:
    """Interact with an AWS service"""

    AVAILABLERESOURCES = ['cloudformation', 'cloudwatch', 'dynamodb', 'ec2', 'glacier', 'iam', 'opsworks', 's3', 'sns',
                          'sqs']

    def __init__(self, service_name, creds_profile_name='default', region_name='us-east-1', logging_obj=None):
        """ Create an AwsInitializer object

        Args:
            service_name: (str) name of the AWS service
            creds_profile_name: (str) name of the profile in the credentials file
            region_name: (str) AWS region to use
            logging_obj: (common.Util.Logging.Logging) logger
        """
        # Set logging level for the third party libraries
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        # Set logging_obj
        if logging_obj is None:
            log_filename = get_log_filepath('Python App')
            logging_obj = Logging(name=__name__, log_filename=log_filename, log_level_str='INFO')
        self.logging_obj = logging_obj
        # Set other inputs as class properties
        self.creds_profile_name = creds_profile_name
        self.region_name = region_name
        # Initialize a client and resource
        session = boto3.Session(profile_name=creds_profile_name, region_name=region_name)
        self.logging_obj.log(self.logging_obj.INFO, "Initializing a {service_name} client".format(
            service_name=service_name))
        self.client = session.client(service_name, verify=False)  # verify=False means that SSL certificates are not verified
        if is_in(service_name, self.AVAILABLERESOURCES):
            self.logging_obj.log(self.logging_obj.INFO, "Initializing a {service_name} resource".format(
                service_name=service_name))
            self.resource = session.resource(service_name, verify=False)
        else:
            self.resource = None


class S3(AwsInitializer):
    """Interact with AWS S3"""

    def __init__(self, creds_profile_name='default', region_name='us-east-1', logging_obj=None):
        """ Create a S3 object

        Initializes a S3 object by using boto 3 to create a client for the s3 service.
        Boto3 is the AWS API for Python.

        In order for boto3 to create a client, credentials must be set up.
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#guide-configuration

        Args:
            logging_obj: (common.Util.Logging.Logging)

        Example:
            from DataAccess.AmazonWebServicesApi import S3
            s3_obj = S3(creds_profile_name='dev')

        """
        super().__init__('s3', creds_profile_name, region_name, logging_obj)

    def list_buckets(self):
        """ List S3 buckets

        Returns:
            self.client.list_buckets(): (dict) S3 buckets
        """
        return self.client.list_buckets()

    def create_bucket(self, bucket_name, location='us-west-1'):
        """ Create a bucket in S3

        Args:
            bucket_name: (str) name of the bucket
            location: (str) location of where the bucket will be hosted

        Returns:
            out: (dict) response metadata
        """
        out = self.client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': location
            }
        )
        self.logging_obj.log(self.logging_obj.INFO, "message='{response_metadata}'".format(response_metadata=out))
        return out

    def delete_bucket(self, bucket_name):
        """ Delete a bucket in S3

        Args:
            bucket_name: (str) name of the bucket

        Returns:
            out: (dict) response metadata
        """
        out = self.client.delete_bucket(Bucket=bucket_name)
        return out


class S3Bucket(S3):
    """Interact with an AWS S3 bucket

    Useful online article: https://realpython.com/python-boto3-aws-s3/

    The pandas/s3 methods were originally taken from the following project:
    https://gist.github.com/uhho/a1490ae2abd112b556dcd539750aa151
    """

    def __init__(self, bucket_name, creds_profile_name='default', region_name='us-east-1', logging_obj=None,
                 create_datetime=None):
        """ Create a DataAccess.AmazonWebServicesApi.S3Bucket object

        Args:
            bucket_name (str): name of the bucket you want to interact with
            creds_profile_name (str): name of the credentials profile to use in ~/.aws/credentials
            region_name (str): name of the AWS region
            logging_obj (common.Util.Logging.Logging): logger
            create_datetime (str): creation datetime stamp to use in S3 keys for uploading new objects

        Example:
            from DataAccess.AmazonWebServicesApi import S3Bucket
            s3_bucket_obj = S3Bucket('usanapa', 'default')
        """
        super().__init__(creds_profile_name, region_name, logging_obj)
        self.bucket_name = bucket_name
        self.bucket = self.resource.Bucket(self.bucket_name)
        if create_datetime is None:
            create_datetime = get_curr_datetime_str()
        self.create_datetime = create_datetime

    def get_bucket_policy(self):
        return self.client.get_bucket_policy(Bucket=self.bucket_name)

    def get_bucket_encryption(self):
        return self.client.get_bucket_encryption(Bucket=self.bucket_name)

    def upload_file(self, full_file_path, key, extra_args_dict=None):
        """ Uploads a file to S3

        This method can be used for uploading very large files as it makes use of parallel processing on multiple threads.

        Args:
            full_file_path: (str)
            key: (str)
            extra_args_dict: (dict)

        Returns:
            None

        Example:
            from DataAccess.AmazonWebServicesApi import S3Bucket
            s3_bucket_obj = S3Bucket('usanapa', 'default')
            s3_bucket_obj.upload_file("data-files/myfile.json", "google_analytics/usana.com/daily_regional_summary/2018-01-05/2019-01-21 05:30", {'ACL': 'public-read', 'ContentType': 'text/json'})
        """
        config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                                multipart_chunksize=1024 * 25, use_threads=True)
        self.resource.meta.client.upload_file(full_file_path, self.bucket_name, key,
                                              ExtraArgs=extra_args_dict,
                                              Config=config)

    def s3_to_pandas_parquet(self, key, **args):
        """ Read a S3 Parquet file to a Pandas DataFrame

        Args:
            key (str):

        Returns:
            df (pandas.DataFrame):

        """
        obj = self.client.get_object(Bucket=self.bucket_name, Key=key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()), **args)
        return df

    def s3_to_pandas_parquets(self, objects_path):
        """ Read multiple Parquet files under a specified path in S3

        Args:
            objects_path (str): path to objects in S3

        Returns:
            df (pandas.DataFrame): result set

        """
        if not objects_path.endswith('/'):
            objects_path = objects_path + '/'  # Add '/' to the end
        key_list = self.get_object_keys(objects_path)
        if not key_list:
            self.logging_obj.log(self.logging_obj.WARN, "message='No files found in {bucket_name}/{prefix}'".format(bucket_name=self.bucket_name,
                                                                                                                    prefix=objects_path))
        dfs = [self.s3_to_pandas_parquet(key) for key in key_list]
        df = pd.concat(dfs, ignore_index=True)
        return df

    def pandas_to_s3(self, df, key):
        """ Put Pandas DataFrame into S3 bucket

        Args:
            df: (pandas.DataFrame) data to put into S3 bucket
            key: (str) key or path to data in S3

        Returns:
            obj: ()

        """
        # write DF to string stream
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # reset stream position
        csv_buffer.seek(0)
        # create binary stream
        gz_buffer = io.BytesIO()

        # compress string stream using gzip
        with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
            gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))

        # write stream to S3
        obj = self.client.put_object(Bucket=self.bucket_name, Key=key, Body=gz_buffer.getvalue())
        return obj

    def pandas_to_s3_parquet(self, df, key, flat_file_path='data-files/mydata.parq'):
        """ Put Pandas DataFrame into S3 bucket in Parquet format

        Args:
            df: (pandas.DataFrame) data to put into S3 bucket
            key: (str) key or path to data in S3
            flat_file_path: path to the location of the Parquet file to be created

        Returns:

        """
        # put DF in a flat file of the format Parquet
        df.to_parquet(flat_file_path)

        # write stream to S3
        self.upload_file(flat_file_path, key)

    def s3_to_pandas(self, key):
        """ Get data as a Pandas DataFrame from S3

        Args:
            key: (str) key or path to data in S3

        Returns:
            df: (pandas.DataFrame) response as a DataFrame
        """
        # get key using boto3 client
        obj = self.client.get_object(Bucket=self.bucket_name, Key=key)
        gz = gzip.GzipFile(fileobj=obj['Body'])

        # load stream directly to DF
        df = pd.read_csv(gz, dtype=str)
        return df

    def s3_to_pandas_with_processing(self, key):
        """ Get data as a Pandas DataFrame from S3

        Args:
            key: (str) key or path to data in S3

        Returns:
            df: (pandas.DataFrame) response as a DataFrame
        """
        # get key using boto3 client
        obj = self.client.get_object(Bucket=self.bucket_name, Key=key)
        gz = gzip.GzipFile(fileobj=obj['Body'])

        # replace some characters in incoming stream and load it to DF
        lines = "\n".join([line.replace('?', ' ') for line in gz.read().decode('utf-8').split('\n')])
        df = pd.read_csv(io.StringIO(lines), dtype=str)
        return df

    def clean_bucket(self, path):
        """ Delete all objects in the bucket within the desired path

        Returns:

        """
        for item in self.bucket.objects.filter(Prefix=path):
            item.delete()

    def get_object_keys(self, prefix):
        """

        Args:
            prefix (str): full path to "directory" to get objects from, e.g. 'google_analytics/daily_site_content/View=1'

        Returns:
            key_list (list of strings): list of keys

        """
        key_list = []
        for obj in self.bucket.objects.filter(Prefix=prefix):
            #print("{key}".format(key=obj.key))
            key_list.append(obj.key)
        return key_list


class Athena(AwsInitializer):
    """Interact with Amazon Athena"""

    def __init__(self,
                 db_name,
                 output_bucket,
                 output_key,
                 creds_profile_name='default', region_name='us-east-1', logging_obj=None):
        """ Initialize an Athena object

        Args:
            db_name: (str) name of database
            output_bucket: (str) name of bucket where Athena should store results
            output_key: (str) key where Athena should store results
            creds_profile_name:
            region_name:
            logging_obj:
        """
        super().__init__('athena', creds_profile_name, region_name, logging_obj)
        self.db_name = db_name
        self.output_bucket = output_bucket
        self.output_key = output_key

    def get_full_output_location_path(self):
        """ Get the full path to Athena's output location

        Returns:
            output_location: (str) full path to Athena's output location

        """
        output_location = 's3://' + self.output_bucket + '/' + self.output_key  # path to output location, e.g. s3://your-bucket/output/path/
        return output_location

    def execute_query(self, query_str):
        """ Execute an Athena query

        Args:
            query_str: (str) query string

        Returns:
            response: (dict) query execution response from Athena

        """
        output_location = self.get_full_output_location_path()
        response = self.client.start_query_execution(QueryString=query_str,
                                                     QueryExecutionContext={'Database': self.db_name},
                                                     ResultConfiguration={
                                                         'OutputLocation': output_location
                                                     })
        return response

    def get_query_execution_id(self, response):
        """ Get the execution ID given an Athena query response

        Args:
            response: (dict) query execution response from Athena

        Returns:
            out: (str) query execution id

        """
        out = response['QueryExecutionId']
        return out

    def get_query_execution_result_info(self, query_execution_id):
        """ Get the query execution result information

        Args:
            query_execution_id: (str) query execution ID

        Returns:
            result: (dict) query execution result

        """
        result = self.client.get_query_execution(QueryExecutionId=query_execution_id)
        return result

    def get_query_output_bucket_key(self, query_execution_info):
        """ Get the bucket name and key of an Athena executed query based on query execution info

        Args:
            query_execution_info: (dict) result from self.get_query_execution_result_info(query_execution_id)

        Returns:
            bucket_name: (str) name of the bucket Athena put the query results into
            key: (str) key where Athena put the query results into

        """
        output_location = query_execution_info['QueryExecution']['ResultConfiguration']['OutputLocation']
        re_str = "s3:\/\/([^\/]*)\/(.*)"
        match = re.search(re_str, output_location)
        bucket_name = match.group(1)
        key = match.group(2)
        return bucket_name, key

    def athena_to_pandas(self, s3_bucket, query_execution_info):
        """ Returns an Athena query result as a Pandas DataFrame

        Args:
            s3_bucket: (DataAccess.AmazonWebServicesApi.S3Bucket) S3 bucket
            query_execution_info: (dict) result from self.get_query_execution_result_info(query_execution_id)

        Returns:
            df: (pandas.DataFrame) Athena query result set

        """
        (bucket_name, key) = self.get_query_output_bucket_key(query_execution_info)
        obj = s3_bucket.client.get_object(Bucket=s3_bucket.bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df

    def get_athena_to_pandas_result(self, s3_bucket, query_execution_info):
        """ Get result from Athena query once it is available in S3

        Args:
            s3_bucket: (DataAccess.AmazonWebServicesApi.S3Bucket) S3 bucket
            query_execution_info: (dict) result from self.get_query_execution_result_info(query_execution_id)

        Returns:
            data_df: (pandas.DataFrame) Athena query result set

        """
        data_df = None
        while data_df is None:
            try:
                data_df = self.athena_to_pandas(s3_bucket, query_execution_info)
            except Exception as ex:
                if str(type(ex)) == "<class 'botocore.errorfactory.NoSuchKey'>":
                    self.logging_obj.log(self.logging_obj.WARN,
                                         "message='Athena query object not yet available in S3! Retrying...'")
                    pass
                else:
                    self.logging_obj.log(self.logging_obj.ERROR,
                                         "message='Problem with getting Athena results back from S3' exception_message={ex_msg}".format(
                                             ex_msg=str(ex)))
                    raise ex
        return data_df

    def get_result_set(self, query_str):
        """ Get Athena query result set as a pandas DataFrame

        Queries Athena. Returns results from S3 as a pandas DataFrame.

        Args:
            query_str: (str) Athena query to execute

        Returns:
            data_df: (pandas.DataFrame) result set from Athena query

        """
        # Execute an Athena query
        response = self.execute_query(query_str=query_str)
        # Get the Athena execution id
        query_execution_id = self.get_query_execution_id(response)
        # Get Athena execution information and metadata
        query_execution_info = self.get_query_execution_result_info(query_execution_id)
        # Create a S3 bucket object for the bucket where Athena has stored the query result data
        athena_s3_bucket_obj = S3Bucket(bucket_name=self.output_bucket,
                                        creds_profile_name=self.creds_profile_name,
                                        region_name=self.region_name,
                                        logging_obj=self.logging_obj)
        # Get Athena query result data as a pandas DataFrame
        data_df = self.get_athena_to_pandas_result(athena_s3_bucket_obj, query_execution_info)
        # Clean up the bucket by deleting all of the Athena query objects
        athena_s3_bucket_obj.clean_bucket(self.output_key)
        return data_df


class Glue(AwsInitializer):
    """Interact with AWS Glue

    Service Documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html
    API Documentation: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-crawling.html

    """

    def __init__(self,
                 creds_profile_name='default', region_name='us-east-1', logging_obj=None):
        """ Initialize a Glue object

        Args:
            creds_profile_name:
            region_name:
            logging_obj:
        """
        super().__init__('glue', creds_profile_name, region_name, logging_obj)


class GlueCrawler(Glue):
    """Interact with an AWS Glue Crawler"""

    READYSTATE = 'READY'

    def __init__(self,
                 crawler_name,
                 creds_profile_name='default', region_name='us-east-1', logging_obj=None):
        """ Initialize a DataAccess.AmazonWebServicesApi.GlueCrawler object

        Args:
            crawler_name (str): name of the crawler
            creds_profile_name:
            region_name:
            logging_obj:
        """
        super().__init__(creds_profile_name, region_name, logging_obj)
        self.crawler_name = crawler_name

    def get_crawler_metadata(self):
        """ Retrieves metadata for the crawler

        Returns:
            crawler_metadata (dict): crawler metadata
                See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-crawling.html#aws-glue-api-crawler-crawling-Crawler

        """
        crawler_metadata = self.client.get_crawler(Name=self.crawler_name)
        return crawler_metadata

    def get_crawler_state(self):
        """ Gets the current state of the crawler

        Returns:

        """
        crawler_metadata = self.get_crawler_metadata()
        crawler_state = crawler_metadata['Crawler']['State']
        return crawler_state

    def get_crawler_metrics(self):
        """ Retrieves metrics about the crawler

        Returns:
            crawler_metrics (dict): crawler metrics

        """
        crawler_metrics = self.client.get_crawler_metrics(CrawlerNameList=[self.crawler_name])
        return crawler_metrics

    def does_crawler_exist(self):
        """ Checks to see if the crawler exists

        Returns:
            out: (bool) indicates whether or not the crawler named self.crawler_name exists

        """
        my_crawlers = self.client.get_crawlers()
        out = False
        for crawler in my_crawlers['Crawlers']:
            crawler_name = crawler['Name']
            if crawler_name == self.crawler_name:
                out = True
        return out

    def create_crawler(self, iam_role_name, database_name, crawler_targets,
                       description='',
                       classifiers=[],
                       schedule='',
                       schema_change_policy=None):
        """ Creates a Glue Crawler

        Args:
            iam_role_name (str): name of the IAM role, e.g. 'MyDefaultGlueServiceRole'
            database_name (str): name of the AWS Glue database
            crawler_targets (dict): CrawlerTargets structure
                See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-crawling.html#aws-glue-api-crawler-crawling-CrawlerTargets
            description (str): description of what data the crawler will scan
            classifiers (list): list of custom classifiers that the user has registered
            schedule (str): A cron expression used to specify the schedule.
                See https://docs.aws.amazon.com/glue/latest/dg/monitor-data-warehouse-schedule.html
                For example, to run something every day at 12:15 UTC, you would specify: cron(15 12 * * ? *).
            schema_change_policy (dict): SchemaChangePolicy object
                See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-crawling.html#aws-glue-api-crawler-crawling-SchemaChangePolicy

        Returns:

        Example:
            from DataAccess.AmazonWebServicesApi import GlueCrawler
            my_glue_crawler = GlueCrawler('tester')
            crawler_targets = {'S3Targets': [{'Path': 's3://usanapa2/google_analytics/daily_regional_summary', 'Exclusions': []}], 'JdbcTargets': [], 'DynamoDBTargets': []}
            my_glue_crawler.create_crawler('MyDefaultGlueServiceRole', 'ga', crawler_targets)

        """
        if schema_change_policy is None:
            schema_change_policy = {'UpdateBehavior': 'UPDATE_IN_DATABASE', 'DeleteBehavior': 'DEPRECATE_IN_DATABASE'}
        if self.does_crawler_exist() is False:
            self.client.create_crawler(Name=self.crawler_name,
                                       Role=iam_role_name,
                                       DatabaseName=database_name,
                                       Targets=crawler_targets,
                                       Description=description,
                                       Classifiers=classifiers,
                                       SchemaChangePolicy=schema_change_policy,
                                       Schedule=schedule)
        else:
            self.logging_obj.log(self.logging_obj.WARN,
                                 "message='The crawler {crawler_name} already exists!'".format(crawler_name=self.crawler_name))

    def start_crawler(self):
        """ Starts the crawler

        Returns:

        """

        crawler_state = self.get_crawler_state()
        if crawler_state == self.READYSTATE:
            self.client.start_crawler(Name=self.crawler_name)
        else:
            self.logging_obj.log(self.logging_obj.WARN,
                                 """message='The crawler {crawler_name} is in the {crawler_state} state 
                                 and not in the READY state meaning it cannot be started. 
                                 Please logon to the AWS console (https://console.aws.amazon.com/console) 
                                 and see the status of this crawler.'
                                 """.format(crawler_name=self.crawler_name,
                                            crawler_state=crawler_state))








