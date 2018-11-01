# coding=utf-8
from datetime import datetime
import time

from sqlalchemy.ext.declarative import declarative_base


class Base(object):
    def columns(self):
        return [c.name for c in self.__table__.columns]

    def to_dict(self):
        return dict([(c, getattr(self, c)) for c in self.columns()])


Model = declarative_base(cls=Base)
