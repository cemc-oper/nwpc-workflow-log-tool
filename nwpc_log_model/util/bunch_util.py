# coding=utf-8
from nwpc_log_model.rdbms_model.models import Record
from nwpc_work_flow_model.sms import Bunch
from sqlalchemy import distinct


class BunchUtil(object):
    def __init__(self):
        pass

    @staticmethod
    def generate_repo_tree_from_session(owner, repo, query_date, session):
        """
        从数据库中某天的日志条目生成Bunch。

        :param owner: repo owner
        :param repo: repo name
        :param query_date: some day
        :param session: sql db connection session for
        :return: bunch object in dict.
        """
        Record.prepare(owner, repo)
        # get node list
        query = session.query(distinct(Record.record_fullname)) \
            .filter(Record.record_date == query_date) \
            .filter(Record.record_fullname.isnot(None)) \
            .order_by(Record.record_fullname)
        records = query.all()
        bunch = Bunch()
        for a_node_path in records:
            bunch.add_node(a_node_path[0])
        # tree = bunch.to_dict()
        return bunch
