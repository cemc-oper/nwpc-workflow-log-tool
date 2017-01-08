# coding=utf-8


class NodeType(object):
    Unknown = 1
    Root = 2
    Suite = 3
    Family = 4
    Task = 5
    Alias = 6
    NonTaskNode = 7
    Meter = 8

    node_type_mapper = {
        Unknown: 'unknown',
        Root: 'root',
        Suite: 'suite',
        Family: 'family',
        Task: 'task',
        Alias: 'alias',
        NonTaskNode: 'non-task',
        Meter: 'meter',
    }

    def __init__(self):
        self.node_state = self.Unknown

    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

    @staticmethod
    def get_node_type_string(node_type):
        return NodeType.node_type_mapper[node_type]


"""
Node:

    status:
        first three characters of the status word.
        unk: unknown
        sus: suspended
        com: complete
        que: queued
        sub: submitted
        act: active
        abo: aborted
        shu: shutdown
        hal: halted
"""
class Node(object):
    def __init__(self):
        self.parent = None
        self.children = list()
        self.name = ''
        self.status = 'unk'

    def __str__(self):
        return self.get_node_path()

    """
        node:
            {
                "name": "node",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "status", "type": "string"},
                    {"name": "path", "type": "string"},
                    {
                        "name": "children",
                        "type": "array",
                        "items": {"type":"node"}
                    }
                ]
            }
    """
    def to_dict(self):
        """

        :return:
            {
                'name': str, node name,
                'node_type': str, node type,
                'node_path': str, node path,
                'children': array, [
                    node dict,
                    node dict,
                    ...
                ]

            }
        """
        ret = dict()
        ret['name'] = self.name
        ret['children'] = list()
        ret['node_type'] = self.get_node_type()

        ret['node_path'] = self.get_node_path()
        ret['path'] = self.get_node_path()

        ret['status'] = self.status

        for a_child in self.children:
            ret['children'].append(a_child.to_dict())
        return ret

    @staticmethod
    def create_from_dict(node_dict, parent=None):
        # use parent is evil.
        node = Node()
        node.parent = parent
        node.name = node_dict['name']
        node.status = node_dict['status']
        node.children = []
        for a_child_item in node_dict['children']:
            a_child_node = Node.create_from_dict(a_child_item, parent=node)
            node.children.append(a_child_node)
        return node

    def add_child(self, node):
        self.children.append(node)

    def get_node_path(self):
        cur_node = self
        node_list = []
        while cur_node is not None:
            node_list.insert(0, cur_node.name)
            cur_node = cur_node.parent
        node_path = "/".join(node_list)
        if node_path == "":
            node_path = "/"
        return node_path

    def get_node_type(self):
        if self.parent is None:
            return NodeType.Root

        if self.parent.parent is None:
            return NodeType.Suite

        if len(self.children) > 0:
            return NodeType.Family
        else:
            if self.name.find(":") != -1:
                return NodeType.NonTaskNode
            else:
                return NodeType.Task

    def get_node_type_string(self):
        return NodeType.get_node_type_string(self.get_node_type())

    def is_leaf(self):
        if len(self.children) == 0:
            return True
        else:
            # check whether child is an alias
            alias_flag = False
            for a_child in self.children:
                if a_child.is_alias():
                    alias_flag = True
                    break
            if alias_flag:
                return True
            else:
                return False

    def is_suite(self):
        if self.parent:
            parent = self.parent
            if parent.parent is None:
                return True
        return False

    def is_alias(self):
        if len(self.children) != 0:
            return False

        if self.name.startswith('alias'):
            return True

        return False
