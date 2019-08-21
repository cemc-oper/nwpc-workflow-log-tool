# coding: utf-8
import logging

from nwpc_workflow_log_model.rmdb.base.model import Model
from nwpc_workflow_log_model.rmdb.base.record import RecordBase
from nwpc_workflow_log_model.base.sms_log_record import SmsLogRecord


logger = logging.getLogger()


class SmsRecordBase(RecordBase):
    """
    SMS日志的基类，表述日志格式。
    使用多个结构相同的表记录SMS日志条目，通过继承该类并修改__tablename__属性实现。
    """

    def __str__(self):
        return "<{class_name}(string='{record_string}')>".format(
            class_name=self.__class__.__name__, record_string=self.log_record.strip()
        )

    def parse(self, line):
        record = SmsLogRecord()
        record.parse(line)

        attrs = [
            'log_type',
            'date',
            'time',
            'command',
            'node_path',
            'additional_information',
            'log_record',
        ]
        for an_attr in attrs:
            setattr(self, an_attr, getattr(record, an_attr))


class SmsRecord(SmsRecordBase, Model):
    __tablename__ = "sms_record"

    def __init__(self):
        pass
