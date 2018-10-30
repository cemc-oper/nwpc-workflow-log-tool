# coding: utf-8
from sqlalchemy import Column, Integer, String, Date, Time, Text


class Record(object):
    """
    SMS日志的基类，表述日志格式。
    使用多个结构相同的表记录SMS日志条目，通过继承该类并修改__tablename__属性实现。
    """
    repo_id = Column(Integer, primary_key=True)
    version_id = Column(Integer, primary_key=True)
    line_no = Column(Integer, primary_key=True)
    record_type = Column(String(100))
    record_date = Column(Date())
    record_time = Column(Time())
    record_command = Column(String(100))
    record_fullname = Column(String(200))
    record_additional_information = Column(Text())
    record_string = Column(Text())

    def __str__(self):
        return "<{class_name}(string='{record_string}')>".format(
            class_name=self.__class__.__name__,
            record_string=self.record_string.strip()
        )

    def parse(self, line):
        pass
