# coding: utf-8


class LogRecord(object):
    def __init__(self):
        self.log_type = None
        self.date = None
        self.time = None
        self.command = None
        self.node_path = None
        self.additional_information = None
        self.log_record = None

    def parse(self, line: str):
        pass
