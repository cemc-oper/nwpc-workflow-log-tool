import typing

from nwpc_workflow_log_model.log_record.log_record import LogRecord
from nwpc_workflow_log_model.analytics.situation_type import (
    FamilySituationType,
    TaskSituationType
)
from nwpc_workflow_log_model.analytics.node_situation import NodeSituation


class SituationRecord(object):
    def __init__(
            self,
            date,
            state: TaskSituationType or FamilySituationType,
            node_situation: NodeSituation,
            records: typing.List[LogRecord],
    ):
        self.date = date
        self.state = state
        self.node_situation = node_situation
        self.records = records
