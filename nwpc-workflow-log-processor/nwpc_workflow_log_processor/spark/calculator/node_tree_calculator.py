# coding=utf-8
from nwpc_workflow_model.ecflow.bunch import Bunch


def calculate_node_tree(config: dict, repo_type: str, record_rdd, spark) -> dict:
    # STEP: map to (date, node_path)
    # record object => (date, node_path)  distinct
    def node_path_map(record):
        return record.date, record.node_path
    date_node_path_rdd = record_rdd.map(node_path_map).distinct()

    # STEP: group by date
    # (record_date, list of record_fullname)
    date_node_path_list_rdd = date_node_path_rdd.groupByKey()

    # STEP: collect and generate bunch
    date_with_node_path_list = date_node_path_list_rdd.collect()

    print("Generating bunch...",)
    bunch_map = {}
    for i in date_with_node_path_list:
        day = i[0]
        node_path_list = i[1]
        bunch = Bunch()
        for node_path in node_path_list:
            if node_path is not None:
                bunch.add_node(node_path)
        print("Generated bunch for ", day)
        bunch_map[day] = bunch

    return bunch_map
