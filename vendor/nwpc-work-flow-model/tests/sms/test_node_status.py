import pytest
from nwpc_work_flow_model.sms import NodeStatus


class TestNodeStatus:
    @classmethod
    def setup_class(cls):
        pass

    def test_construction(self):
        node_status = NodeStatus.Unknown
        assert node_status == NodeStatus.Unknown

        node_status = NodeStatus('act')
        assert node_status == NodeStatus.Active

        node_status = NodeStatus['Active']
        assert node_status == NodeStatus.Active

        with pytest.raises(ValueError):
            NodeStatus('non-exist-status')

    def test_get_node_status(self):
        mapper = {
            'unk': NodeStatus.Unknown,
            'sus': NodeStatus.Suspend,
            'com': NodeStatus.Complete,
            'que': NodeStatus.Queued,
            'sub': NodeStatus.Submitted,
            'act': NodeStatus.Active,
            'abo': NodeStatus.Aborted,
            'shu': NodeStatus.Shutdown,
            'hal': NodeStatus.Halted
        }

        result_list = [NodeStatus.get_node_status(i) == mapper[i] for i in mapper]
        assert all(result_list)

        result_list = [NodeStatus(i) == mapper[i] for i in mapper]
        assert all(result_list)

        assert NodeStatus.get_node_status('non-exist-status') is None
