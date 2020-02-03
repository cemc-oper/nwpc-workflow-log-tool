# coding: utf-8
import findspark
from pyspark.sql import SparkSession
from pyspark import SparkConf


def create_local_file_session(config: dict) -> SparkSession:
    engine_config = config["engine"]

    findspark.init(engine_config["spark_home"])
    config_pairs = list(engine_config["spark_config"].items())
    conf = SparkConf().setAll(config_pairs)
    spark = SparkSession \
        .builder \
        .appName(engine_config["app_name"]) \
        .master("local[4]") \
        .config(conf=conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel(engine_config["log_level"])
    return spark


def create_mysql_session(config: dict) -> SparkSession:
    engine_config = config["engine"]
    findspark.init(engine_config["spark_home"])
    config_pairs = list(engine_config["spark_config"].items())
    conf = SparkConf().setAll(config_pairs)
    spark = SparkSession \
        .builder \
        .appName(engine_config["app_name"]) \
        .master("local[4]") \
        .config(conf=conf) \
        .getOrCreate()
    # .config("spark.driver.extraClassPath", config['datastore']['mysql']['driver']) \
    # .config("spark.executor.extraClassPath", config['datastore']['mysql']['driver']) \
    # .config("spark.executor.memory", '4g') \
    # .config("spark.driver.memory", '4g') \
    spark.sparkContext.setLogLevel(engine_config["log_level"])
    return spark
