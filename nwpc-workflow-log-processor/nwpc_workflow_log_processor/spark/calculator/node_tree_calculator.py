# coding=utf-8
from pyspark.sql import SparkSession
from pyspark import RDD
from loguru import logger

from nwpc_workflow_model.sms.bunch import Bunch as SmsBunch
from nwpc_workflow_model.ecflow.bunch import Bunch as EcflowBunch


def calculate_node_tree(
        config: dict,
        record_rdd: RDD,
        spark: SparkSession,
        repo_type: str = "ecflow",
) -> dict:
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
    bunch_class = _get_bunch_class(repo_type)
    bunch_map = {}
    for i in date_with_node_path_list:
        day = i[0]
        node_path_list = i[1]
        bunch = bunch_class()
        for node_path in node_path_list:
            if node_path is not None:
                bunch.add_node(node_path)
        logger.info("Generated bunch for ", day)
        bunch_map[day] = bunch

    return bunch_map


def _get_bunch_class(repo_type: str):
    if repo_type == "ecflow":
        return EcflowBunch
    elif repo_type == "sms":
        return SmsBunch
    else:
        raise NotImplemented(f"repo_type is not supported: {repo_type}")
