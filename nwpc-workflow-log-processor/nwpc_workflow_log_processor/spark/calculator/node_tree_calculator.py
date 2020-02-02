# coding=utf-8
from pyspark.sql import SparkSession
from pyspark import RDD
from loguru import logger

from nwpc_workflow_log_processor.common.util import get_bunch_class


def calculate_node_tree(
        config: dict,
        record_rdd: RDD,
        spark: SparkSession,
        repo_type: str = "ecflow",
) -> dict:
    """Calculate node tree for each date in record RDDs.

    Parameters
    ----------
    config: dict
        processor's config
    record_rdd: RDD
        records RDD
    spark: SparkSession
        spark session
    repo_type: ["ecflow", "sms"]
        repo type

    Returns
    -------
    dict
        bunch map dict, key: date, value: bunch map
    """
    # **STEP**: map to (date, node_path)
    #   record object => (date, node_path) distinct
    def node_path_map(record):
        return record.date, record.node_path
    date_node_path_rdd = record_rdd.map(node_path_map).distinct()

    # **STEP**: group by date
    #   (date, node_path) => (record_date, list of record_fullname)
    date_node_path_list_rdd = date_node_path_rdd.groupByKey()

    # **STEP**: collect
    date_with_node_path_list = date_node_path_list_rdd.collect()

    # **STEP**: generate bunch
    logger.info("Generating bunch...")
    bunch_class = get_bunch_class(repo_type)
    bunch_map = {}
    for (day, node_path_list) in date_with_node_path_list:
        bunch = bunch_class()
        for node_path in node_path_list:
            if node_path is not None:
                bunch.add_node(node_path)
        logger.info(f"Generating bunch...done for {day}")
        bunch_map[day] = bunch

    return bunch_map
