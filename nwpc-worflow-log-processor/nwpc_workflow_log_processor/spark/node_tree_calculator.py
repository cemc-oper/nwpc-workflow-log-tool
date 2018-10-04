# coding=utf-8
from nwpc_work_flow_model.sms import Bunch


def calculate_node_tree(config: dict, record_rdd, spark) -> dict:
    # record object => (record_date, record_fullname)  distinct
    def node_path_map(record):
        return record.record_date, record.record_fullname

    date_node_path_rdd = record_rdd.map(node_path_map).distinct()

    # (record_date, list of record_fullname)
    date_node_path_list_rdd = date_node_path_rdd.groupByKey()

    date_with_node_path_list = date_node_path_list_rdd.collect()

    print("Generating bunch...",)
    # 日期 [ start_date - 1, end_date ]
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
