import unittest

from nwpc_work_flow_model.sms import Node, NodeType


class TestNode(unittest.TestCase):
    def setUp(self):
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
        self.root = Node()

        self.suite1 = Node(name='suite1')
        self.suite2 = Node(name='suite2')
        self.suite3 = Node(name='suite3')
        self.suite4 = Node(name='suite4')

        self.family1 = Node(name='family1')
        self.family2 = Node(name='family2')
        self.family3 = Node(name='family3')
        self.family4 = Node(name='family4')
        self.family5 = Node(name='family5')
        self.family6 = Node(name='family6')

        self.task1 = Node(name='task1')
        self.task2 = Node(name='task2')
        self.task3 = Node(name='task3')
        self.task4 = Node(name='task4')
        self.task5 = Node(name='task5')
        self.task6 = Node(name='task6')
        self.task7 = Node(name='task7')
        self.task8 = Node(name='task8')
        self.task9 = Node(name='task9')
        self.task10 = Node(name='task10')
        self.task11 = Node(name='task11')
        self.task12 = Node(name='task12')

        self.alias0 = Node('alias0')

        for node in [self.suite1, self.suite2, self.suite3, self.suite4]:
            self.root.add_child(node)

        for node in [self.family1, self.family2, self.task6]:
            self.suite1.add_child(node)

        for node in [self.task1, self.task2, self.task3]:
            self.family1.add_child(node)

        for node in [self.task4, self.task5]:
            self.family2.add_child(node)

        for node in [self.family3]:
            self.suite2.add_child(node)

        for node in [self.task7, self.task8]:
            self.family3.add_child(node)

        for node in [self.family4, self.family6]:
            self.suite3.add_child(node)

        for node in [self.family5, self.task10]:
            self.family4.add_child(node)

        for node in [self.task9]:
            self.family5.add_child(node)

        for node in [self.task11]:
            self.family6.add_child(node)

        self.suite4.add_child(self.task12)
        self.task12.add_child(self.alias0)

    def test_construction(self):
        node = Node()
        self.assertIsNone(node.parent)
        self.assertIsInstance(node.children, list)
        self.assertEqual(len(node.children), 0)
        self.assertEqual(node.name, '')
        self.assertEqual(node.status, 'unk')

    def test_add_child(self):
        self.assertEqual(
            self.root.children,
            [self.suite1, self.suite2, self.suite3, self.suite4]
        )

        self.assertEqual(
            self.suite1.children,
            [self.family1, self.family2, self.task6]
        )

        self.assertEqual(
            self.family1.children,
            [self.task1, self.task2, self.task3]
        )

        self.assertEqual(self.task1.children, [])

    def test_get_node_path(self):
        self.assertEqual(self.root.get_node_path(), '/')

        self.assertEqual(self.suite1.get_node_path(), '/suite1')
        self.assertEqual(self.suite2.get_node_path(), '/suite2')

        self.assertEqual(self.family1.get_node_path(), '/suite1/family1')
        self.assertEqual(self.family2.get_node_path(), '/suite1/family2')
        self.assertEqual(self.family3.get_node_path(), '/suite2/family3')
        self.assertEqual(self.family5.get_node_path(), '/suite3/family4/family5')

        self.assertEqual(self.task1.get_node_path(), '/suite1/family1/task1')
        self.assertEqual(self.task2.get_node_path(), '/suite1/family1/task2')
        self.assertEqual(self.task6.get_node_path(), '/suite1/task6')
        self.assertEqual(self.task12.get_node_path(), '/suite4/task12')

        self.assertEqual(self.alias0.get_node_path(), '/suite4/task12/alias0')

    def test_get_node_type(self):
        self.assertEqual(self.root.get_node_type(), NodeType.Root)
        self.assertEqual(self.root.get_node_type_string(), 'root')

        for node in [getattr(self, 'suite' + str(i)) for i in range(1, 5)]:
            self.assertEqual(node.get_node_type(), NodeType.Suite)
            self.assertEqual(node.get_node_type_string(), 'suite')

        for node in [getattr(self, 'family' + str(i)) for i in range(1, 7)]:
            self.assertEqual(node.get_node_type(), NodeType.Family)
            self.assertEqual(node.get_node_type_string(), 'family')

        for node in [getattr(self, 'task' + str(i)) for i in range(1, 13)]:
            self.assertEqual(node.get_node_type(), NodeType.Task)
            self.assertEqual(node.get_node_type_string(), 'task')

        self.assertEqual(self.alias0.get_node_type(), NodeType.Alias)
        self.assertEqual(self.alias0.get_node_type_string(), 'alias')

    def test_is_leaf(self):
        self.assertFalse(self.root.is_leaf())

        for node in [getattr(self, 'suite' + str(i)) for i in range(1, 5)]:
            self.assertFalse(node.is_leaf())

        for node in [getattr(self, 'family' + str(i)) for i in range(1, 7)]:
            self.assertFalse(node.is_leaf())

        for node in [getattr(self, 'task' + str(i)) for i in range(1, 13)]:
            self.assertTrue(node.is_leaf())

        self.assertTrue(self.alias0.is_leaf())

    def test_is_suite(self):
        self.assertFalse(self.root.is_suite())

        for node in [getattr(self, 'suite' + str(i)) for i in range(1, 5)]:
            self.assertTrue(node.is_suite())

        for node in [getattr(self, 'family' + str(i)) for i in range(1, 7)]:
            self.assertFalse(node.is_suite())

        for node in [getattr(self, 'task' + str(i)) for i in range(1, 13)]:
            self.assertFalse(node.is_suite())

        self.assertFalse(self.alias0.is_suite())

    def test_is_alias(self):
        self.assertFalse(self.root.is_alias())

        for node in [getattr(self, 'suite' + str(i)) for i in range(1, 5)]:
            self.assertFalse(node.is_alias())

        for node in [getattr(self, 'family' + str(i)) for i in range(1, 7)]:
            self.assertFalse(node.is_alias())

        for node in [getattr(self, 'task' + str(i)) for i in range(1, 13)]:
            self.assertFalse(node.is_alias())

        self.assertTrue(self.alias0.is_alias())

    def test_str(self):
        self.assertEqual(str(self.root), self.root.get_node_path())
        self.assertEqual(str(self.suite1), self.suite1.get_node_path())
        self.assertEqual(str(self.family1), self.family1.get_node_path())
        self.assertEqual(str(self.task1), self.task1.get_node_path())

    def test_to_dict(self):
        root_dict = self.root.to_dict()
        self.assertEqual(len(root_dict['children']), 4)
        del root_dict['children']
        self.assertEqual(
            root_dict,
            {
                'name': '',
                'node_type': NodeType.Root,
                'node_path': '/',
                'path': '/',
                'status': 'unk'
            }
        )

        family1_dict = self.family1.to_dict()
        self.assertEqual(len(family1_dict['children']), 3)
        del family1_dict['children']
        self.assertEqual(
            family1_dict,
            {
                'name': 'family1',
                'node_type': NodeType.Family,
                'node_path': '/suite1/family1',
                'path': '/suite1/family1',
                'status': 'unk'
            }
        )

        task1_dict_required = {
            'name': 'task1',
            'children': [],
            'node_type': NodeType.Task,
            'node_path': '/suite1/family1/task1',
            'path': '/suite1/family1/task1',
            'status': 'unk'
        }
        self.assertEqual(self.task1.to_dict(), task1_dict_required)

        alias_dict_required = {
            'name': 'alias0',
            'children': [],
            'node_type': NodeType.Alias,
            'node_path': '/suite4/task12/alias0',
            'path': '/suite4/task12/alias0',
            'status': 'unk'
        }
        self.assertEqual(self.alias0.to_dict(), alias_dict_required)

    def test_create_from_dict(self):
        bunch_dict = {
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
        root = Node.create_from_dict(bunch_dict)
        suite1 = root.children[0]
        family1 = suite1.children[0]
        task1 = family1.children[0]

        self.assertEqual(root.name, '')
        self.assertEqual(suite1.name, 'suite1')
        self.assertEqual(family1.name, 'family1')
        self.assertEqual(task1.name, 'task1')

        self.assertEqual(root.get_node_path(), '/')
        self.assertEqual(suite1.get_node_path(), '/suite1')
        self.assertEqual(family1.get_node_path(), '/suite1/family1')
        self.assertEqual(task1.get_node_path(), '/suite1/family1/task1')

        self.assertEqual(root.status, 'act')
        self.assertEqual(suite1.status, 'act')
        self.assertEqual(family1.status, 'act')
        self.assertEqual(task1.status, 'act')

        self.assertEqual(root.parent, None)
        self.assertEqual(suite1.parent, root)
        self.assertEqual(family1.parent, suite1)
        self.assertEqual(task1.parent, family1)
