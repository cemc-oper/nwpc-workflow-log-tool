# coding: utf-8
from enum import Enum


class NodeStatus(Enum):
    Unknown = 'unk'
    Suspend = 'sus'
    Complete = 'com'
    Queued = 'que'
    Submitted = 'sub'
    Active = 'act'
    Aborted = 'abo'
    Shutdown = 'shu'
    Halted = 'hal'

    @staticmethod
    def get_node_status(status_string):
        if len(status_string) == 3:
            return NodeStatus(status_string)
        elif len(status_string) == 6:
            return NodeStatus[status_string]
        else:
            return None
