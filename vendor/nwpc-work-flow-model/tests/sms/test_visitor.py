from nwpc_work_flow_model.sms import Bunch, Node, NodeVisitor, pre_order_travel, SimplePrintVisitor


class NodeListVisitor(NodeVisitor):
    def __init__(self, node_list):
        NodeVisitor.__init__(self)
        self.node_list = node_list

    def visit(self, node):
        self.node_list.append(node)


class NodeDictListVisitor(NodeVisitor):
    def __init__(self, node_list):
        NodeVisitor.__init__(self)
        self.node_list = node_list

    def visit(self, node):
        self.node_list.append(node['path'])


class TestVisitor:
    @classmethod
    def setup_class(cls):
        """
                test node tree structure:
                    root -- suite1 -- family1 -- task1
                         |         |          |- task2
                         |         |          |- task3
                         |         |- family2 -- task4
                         |         |          |- task5
                         |         |- task6
                         |- suite2 -- family3 -- task7
                         |                    |- task8
                         |- suite3 -- family4 -- family5 -- task9
                         |         |          |- task10
                         |         |- family6 -- task11
                         |- suite4 -- task12  -- alias0
                """
        cls.root = Bunch()

        cls.suite1 = Node(name='suite1')
        cls.suite2 = Node(name='suite2')
        cls.suite3 = Node(name='suite3')
        cls.suite4 = Node(name='suite4')

        cls.family1 = Node(name='family1')
        cls.family2 = Node(name='family2')
        cls.family3 = Node(name='family3')
        cls.family4 = Node(name='family4')
        cls.family5 = Node(name='family5')
        cls.family6 = Node(name='family6')

        cls.task1 = Node(name='task1')
        cls.task2 = Node(name='task2')
        cls.task3 = Node(name='task3')
        cls.task4 = Node(name='task4')
        cls.task5 = Node(name='task5')
        cls.task6 = Node(name='task6')
        cls.task7 = Node(name='task7')
        cls.task8 = Node(name='task8')
        cls.task9 = Node(name='task9')
        cls.task10 = Node(name='task10')
        cls.task11 = Node(name='task11')
        cls.task12 = Node(name='task12')

        cls.alias0 = Node('alias0')

        for node in [cls.suite1, cls.suite2, cls.suite3, cls.suite4]:
            cls.root.add_child(node)

        for node in [cls.family1, cls.family2, cls.task6]:
            cls.suite1.add_child(node)

        for node in [cls.task1, cls.task2, cls.task3]:
            cls.family1.add_child(node)

        for node in [cls.task4, cls.task5]:
            cls.family2.add_child(node)

        for node in [cls.family3]:
            cls.suite2.add_child(node)

        for node in [cls.task7, cls.task8]:
            cls.family3.add_child(node)

        for node in [cls.family4, cls.family6]:
            cls.suite3.add_child(node)

        for node in [cls.family5, cls.task10]:
            cls.family4.add_child(node)

        for node in [cls.task9]:
            cls.family5.add_child(node)

        for node in [cls.task11]:
            cls.family6.add_child(node)

        cls.suite4.add_child(cls.task12)
        cls.task12.add_child(cls.alias0)

        cls.bunch_dict = {
            "name": "",
            "children": [
                {
                    "name": "suite1",
                    "children": [
                        {
                            "name": "family1",
                            "children": [
                                {
                                    "name": "task1",
                                    "children": [],
                                    "node_type": 5,
                                    "node_path": "/suite1/family1/task1",
                                    "path": "/suite1/family1/task1",
                                    "status": "act"
                                }
                            ],
                            "node_type": 4,
                            "node_path": "/suite1/family1",
                            "path": "/suite1/family1",
                            "status": "act"
                        }
                    ],
                    "node_type": 3,
                    "node_path": "/suite1",
                    "path": "/suite1",
                    "status": "act"
                }
            ],
            "node_type": 2,
            "node_path": "/",
            "path": "/",
            "status": "act"
        }

    def test_node_visitor(self):
        node_visitor = NodeVisitor()
        pre_order_travel(self.root, node_visitor)

    def test_simple_print_visitor(self):
        print()
        node_visitor = SimplePrintVisitor()
        pre_order_travel(self.root, node_visitor)

    def test_pre_order_travel(self):
        node_list = []
        visitor = NodeListVisitor(node_list)
        pre_order_travel(self.root, visitor)

        expected_node_list = [
            self.root,
            self.suite1,
            self.family1,
            self.task1,
            self.task2,
            self.task3,
            self.family2,
            self.task4,
            self.task5,
            self.task6,
            self.suite2,
            self.family3,
            self.task7,
            self.task8,
            self.suite3,
            self.family4,
            self.family5,
            self.task9,
            self.task10,
            self.family6,
            self.task11,
            self.suite4,
            self.task12,
            self.alias0
        ]
        test_result_list = [node_list[i]==expected_node_list[i] for i in range(0, len(expected_node_list))]
        # print(test_result_list)
        assert all(test_result_list)

    def pre_order_travel_dict(self):
        node_list = []
        visitor = NodeDictListVisitor(node_list)
        pre_order_travel(self.bunch_dict, visitor)

        expected_result = [
            '/',
            '/suite1',
            '/suite1/family1',
            '/suite1/family1/task1',
        ]
        test_result_list = [node_list[i] == expected_result[i] for i in range(0, len(expected_result))]
        assert all(test_result_list)
