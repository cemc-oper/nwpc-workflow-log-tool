# coding: utf-8
import logging

from sqlalchemy import Column, String
from nwpc_workflow_log_model.rmdb.base.model import Model
from nwpc_workflow_log_model.rmdb.base.record import RecordBase
from nwpc_workflow_log_model.base.ecflow_log_record import EcflowLogRecord

logger = logging.getLogger()


class EcflowRecordBase(RecordBase):
    command_type = Column(String(10))

    def parse(self, line):
        record = EcflowLogRecord()
        record.parse(line)

        attrs = [
            'log_type',
            'date',
            'time',
            'command',
            'node_path',
            'additional_information',
            'log_record',
            'command_type',
        ]
        for an_attr in attrs:
            setattr(self, an_attr, getattr(record, an_attr))


class EcflowRecord(EcflowRecordBase, Model):
    __tablename__ = "ecflow_record"

    def __init__(self):
        pass
