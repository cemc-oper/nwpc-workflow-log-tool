# coding: utf-8
import pytest
from nwpc_work_flow_model.sms import NodeType


class TestNodeType:

    def test_construction(self):
        node_type = NodeType.Unknown
        assert node_type == NodeType.Unknown

        node_type = NodeType['Unknown']
        assert node_type == NodeType.Unknown

        node_type = NodeType('unknown')
        assert node_type == NodeType.Unknown

    def test_get_node_type_string(self):
        assert NodeType.get_node_type_string(NodeType.Unknown) == 'unknown'
        assert NodeType.get_node_type_string(NodeType.Root) == 'root'
        assert NodeType.get_node_type_string(NodeType.Suite) == 'suite'
        assert NodeType.get_node_type_string(NodeType.Family) == 'family'
        assert NodeType.get_node_type_string(NodeType.Task) == 'task'
        assert NodeType.get_node_type_string(NodeType.Alias) == 'alias'
        assert NodeType.get_node_type_string(NodeType.NonTaskNode) == 'non-task'
        assert NodeType.get_node_type_string(NodeType.Meter) == 'meter'
