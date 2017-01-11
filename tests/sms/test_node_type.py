import unittest

from nwpc_work_flow_model.sms import NodeType


class TestNodeType(unittest.TestCase):

    def test_construction(self):
        node_type = NodeType()
        self.assertEqual(node_type.node_state, NodeType.Unknown)

    def test_set_node_state(self):
        node_type = NodeType()
        node_type.node_state = NodeType.Task

    def test_get_node_type_string(self):
        self.assertEqual(NodeType.get_node_type_string(NodeType.Unknown), 'unknown')
        self.assertEqual(NodeType.get_node_type_string(NodeType.Root), 'root')
        self.assertEqual(NodeType.get_node_type_string(NodeType.Suite), 'suite')
        self.assertEqual(NodeType.get_node_type_string(NodeType.Family), 'family')
        self.assertEqual(NodeType.get_node_type_string(NodeType.Task), 'task')
        self.assertEqual(NodeType.get_node_type_string(NodeType.Alias), 'alias')
        self.assertEqual(NodeType.get_node_type_string(NodeType.NonTaskNode), 'non-task')
        self.assertEqual(NodeType.get_node_type_string(NodeType.Meter), 'meter')

        self.assertIsNone(NodeType.get_node_type_string('error-node-type'))
