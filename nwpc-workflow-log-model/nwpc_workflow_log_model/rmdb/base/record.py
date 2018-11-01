# coding: utf-8
from sqlalchemy import Column, Integer, String, Date, Time, Text


class RecordBase(object):
    repo_id = Column(Integer, primary_key=True)
    version_id = Column(Integer, primary_key=True)
    line_no = Column(Integer, primary_key=True)
    log_type = Column(String(100))
    date = Column(Date())
    time = Column(Time())
    command = Column(String(100))
    node_path = Column(String(200))
    additional_information = Column(Text())
    log_record = Column(Text())

    def __str__(self):
        return "<{class_name}(string='{record_string}')>".format(
            class_name=self.__class__.__name__,
            record_string=self.log_record.strip()
        )

    def parse(self, line):
        pass
