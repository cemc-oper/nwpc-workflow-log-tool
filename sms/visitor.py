class NodeVisitor(object):
    def __init__(self):
        pass

    def visit(self, node):
        pass

    def before_visit_child(self):
        pass

    def after_visit_child(self):
        pass


class SimplePrintVisitor(NodeVisitor):
    def __init__(self):
        NodeVisitor.__init__(self)
        self.level = 0

    def visit(self, node):
        print("{place_holder}|-{node_name}".format(
            place_holder=" "*self.level,
            node_name=node.name
        ))

    def before_visit_child(self):
        self.level += 1

    def after_visit_child(self):
        self.level -= 1


class ErrorStatusTaskVisitor(NodeVisitor):
    def __init__(self):
        NodeVisitor.__init__(self)
        self.level = 0
        self.error_task_list = []

    def visit(self, node):
        if node.status == 'abo' and node.is_leaf():
            self.error_task_list.append(node)

    def before_visit_child(self):
        self.level += 1

    def after_visit_child(self):
        self.level -= 1


class SubTreeNodeVisitor(NodeVisitor):
    def __init__(self, max_depth):
        NodeVisitor.__init__(self)
        self.level = 0
        self.max_depth = max_depth

    def visit(self, node):
        if self.level == self.max_depth:
            del node['children']
            node['children'] = list()

    def before_visit_child(self):
        self.level += 1

    def after_visit_child(self):
        self.level -= 1


def pre_order_travel(root_node, visitor):
    visitor.visit(root_node)
    for child_node in root_node.children:
        visitor.before_visit_child()
        pre_order_travel(child_node, visitor)
        visitor.after_visit_child()


def pre_order_travel_dict(root_dict, visitor):
    visitor.visit(root_dict)
    for child_node in root_dict['children']:
        visitor.before_visit_child()
        pre_order_travel_dict(child_node, visitor)
        visitor.after_visit_child()
