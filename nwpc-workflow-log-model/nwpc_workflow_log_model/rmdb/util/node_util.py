# coding=utf-8
from nwpc_workflow_model.node_type import NodeType


class NodeUtil(object):
    def __int__(self):
        pass

    @classmethod
    def node_is_valid_from_session(cls, session, owner, repo, node_path, record_class):
        record_class.prepare(owner, repo)
        query = session.query(record_class).filter(record_class.node_path == node_path).limit(1).all()
        if len(query) == 0:
            return False
        else:
            return True

    @classmethod
    def sub_node_is_valid_from_session(cls, session, owner, repo, node_path, record_class):
        # NOTE: 花费太长时间，最好由 Spark 任务得到节点树后查找是否有子节点。
        record_class.prepare(owner, repo)
        path = node_path + '/'
        query = session.query(record_class.node_path) \
            .filter("SUBSTR(record_fullname, 1, {path_length}) = '{path}'".format(path_length=len(path), path=path)) \
            .filter("SUBSTR(record_fullname, 1, {alias_path_length}) != '{alias_path}'".format(
                alias_path_length=len(path + 'alias'), alias_path=path + 'alias')) \
            .limit(1)
        print(query)
        query = query.all()
        if len(query) == 0:
            return False
        else:
            return True

    @classmethod
    def get_node_type_from_session(cls, session, owner, repo, node_path, record_class):
        # NOTE：花费时间太长，最好由 Spark 任务得到节点树后再确定节点类型。
        # TODO: windroc, 2017.08.28, add support for Trigger, Event and so on.
        node_type = NodeType.get_node_type_string(NodeType.Unknown)
        if node_path == '/':
            return NodeType.get_node_type_string(NodeType.Root)
        if not node_path.startswith('/'):
            return node_type
        # test whether the node is exists
        if not NodeUtil.node_is_valid_from_session(session, owner, repo, node_path, record_class):
            return node_type

        if node_path.find('/', 1) == -1:
            return NodeType.get_node_type_string(NodeType.Suite)

        if NodeUtil.sub_node_is_valid_from_session(session, owner, repo, node_path, record_class):
            return NodeType.get_node_type_string(NodeType.Family)
        else:
            if node_path.find(":") != -1:
                return NodeType.get_node_type_string(NodeType.NonTaskNode)
            else:
                return NodeType.get_node_type_string(NodeType.Task)
