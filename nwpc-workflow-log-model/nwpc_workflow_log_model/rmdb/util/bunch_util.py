# coding=utf-8
from sqlalchemy import distinct

from nwpc_workflow_model.bunch import Bunch
from nwpc_workflow_log_model.rmdb.base.record import RecordBase


class BunchUtil(object):
    def __init__(self):
        pass

    @classmethod
    def generate_repo_tree_from_session(cls, session, owner: str, repo: str, query_date, record_class: RecordBase) \
            -> Bunch:
        """
        从数据库中某天的日志条目生成Bunch。

        :param session: sql db connection session
        :param owner: repo owner
        :param repo: repo name
        :param query_date: some day
        :param record_class: class for record
        :return: bunch object in dict.
        """
        record_class.prepare(owner, repo)
        # get node list
        query = session.query(distinct(record_class.node_path)) \
            .filter(record_class.date == query_date) \
            .filter(record_class.node_path.isnot(None)) \
            .order_by(record_class.node_path)
        records = query.all()
        bunch = Bunch()
        for a_node_path in records:
            bunch.add_node(a_node_path[0])
        # tree = bunch.to_dict()
        return bunch
