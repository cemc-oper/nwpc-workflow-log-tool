import unittest

from nwpc_work_flow_model.sms import Bunch, Node, NodeStatus


class TestBunch(unittest.TestCase):
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
        :return: none
        """

        self.root = Bunch()

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

        self.alias0 = Node(name='alias0')

    def tearDown(self):
        del self.root
        for i in range(1, 5):
            delattr(self, 'suite'+str(i))

        for i in range(1, 7):
            delattr(self, 'family'+str(i))

        for i in range(1, 13):
            delattr(self, 'task'+str(i))

        for i in range(0, 1):
            delattr(self, 'alias'+str(i))

    def test_construction(self):
        self.assertEqual(self.root.status, NodeStatus.Unknown)
        self.assertIsNone(self.root.parent)
        self.assertEqual(self.root.children, [])
        self.assertEqual(self.root.name, '')

    def test_add_node(self):
        task1 = self.root.add_node('/suite1/family1/task1')
        task4 = self.root.add_node('/suite1/family2/task4')
        task7 = self.root.add_node('/suite2/family3/task7')
        family3 = self.root.add_node('/suite2/family3')

        self.assertEqual(task7.parent, family3)

        family1 = task1.parent
        family2 = task4.parent
        suite1 = family1.parent
        suite2 = family3.parent

        self.assertEqual(len(self.root.children), 2)
        self.assertEqual(suite1.parent, self.root)
        self.assertEqual(suite2.parent, self.root)
        self.assertEqual(family2.parent, suite1)

    def test_add_node_status(self):
        task1 = self.root.add_node_status({
            'path': '/suite1/family1/task1',
            'status': 'com',
            'name': 'task1'
        })
        task4 = self.root.add_node_status({
            'path': '/suite1/family2/task4',
            'status': 'act',
            'name': 'task4'
        })
        task7 = self.root.add_node_status({
            'path': '/suite2/family3/task7',
            'status': 'que',
            'name': 'task7'
        })
        family3 = self.root.add_node_status({
            'path': '/suite2/family3',
            'status': 'que',
            'name': 'family3'
        })

        family1 = task1.parent
        family2 = task4.parent
        suite1 = family1.parent
        suite2 = family3.parent

        self.assertEqual(len(self.root.children), 2)
        self.assertEqual(suite1.parent, self.root)
        self.assertEqual(suite2.parent, self.root)
        self.assertEqual(family2.parent, suite1)

        self.assertEqual(task1.status, 'com')
        self.assertEqual(task7.status, 'que')
        self.assertEqual(family3.status, 'que')

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
        root = Bunch.create_from_dict(bunch_dict)
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

        self.assertEqual(root.status, NodeStatus.Active)
        self.assertEqual(suite1.status, NodeStatus.Active)
        self.assertEqual(family1.status, NodeStatus.Active)
        self.assertEqual(task1.status, NodeStatus.Active)

        self.assertEqual(root.parent, None)
        self.assertEqual(suite1.parent, root)
        self.assertEqual(family1.parent, suite1)
        self.assertEqual(task1.parent, family1)
