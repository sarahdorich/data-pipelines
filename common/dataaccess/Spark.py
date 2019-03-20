#!/usr/bin/python

""" Wrappers for pyspark

Contains useful wrappers on top of pyspark

"""

import pyspark
from pyspark import sql


class PySpark4Aws:

    def __init__(self, app_name, aws_access_key_id, aws_secret_access_key, aws_endpoint="apigateway.us-east-1.amazonaws.com"):
        conf = pyspark.SparkConf().setAppName(app_name)
        spark_context = pyspark.SparkContext.getOrCreate(conf=conf)
        spark_context._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key_id)
        spark_context._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", aws_endpoint)
        self.spark_context = spark_context
        self.spark_session = sql.SparkSession.builder.getOrCreate()

    def create_spark_df_from_pandas(self, pandas_df):
        spark_df = self.spark_session.createDataFrame(pandas_df)
        return spark_df

    def optimize_spark_pandas_conversions(self):
        """Optimize spark pandas conversions

        This is based off the article here:
        https://docs.databricks.com/spark/latest/spark-sql/spark-pandas.html

        Returns:

        """
        # Enable Arrow-based columnar data transfers
        self.spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")

