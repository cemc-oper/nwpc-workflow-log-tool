# coding: utf-8
import findspark
from pyspark.sql import SparkSession


def create_local_file_session(config):
    findspark.init(config['engine']['spark']['base'])
    spark = SparkSession \
        .builder \
        .appName("sms.spark.nwpc-workflow-log-processor") \
        .master("local[4]") \
        .config("spark.executor.memory", '4g') \
        .getOrCreate()
    return spark


def create_mysql_session(config):
    findspark.init(config['engine']['spark']['base'])
    spark = SparkSession \
        .builder \
        .appName("sms.spark.nwpc-workflow-log-processor") \
        .master("local[4]") \
        .config("spark.driver.extraClassPath", config['datastore']['mysql']['driver']) \
        .config("spark.executor.extraClassPath", config['datastore']['mysql']['driver']) \
        .config("spark.executor.memory", '4g') \
        .config("spark.driver.memory", '4g') \
        .getOrCreate()
    return spark
