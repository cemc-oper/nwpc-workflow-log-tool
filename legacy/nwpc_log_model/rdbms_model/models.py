# coding=utf-8
from datetime import datetime
import time

from sqlalchemy import Column, Integer, String, Text, Date, Time, Index
from sqlalchemy.ext.declarative import declarative_base, declared_attr


class Base(object):
    def columns(self):
        return [c.name for c in self.__table__.columns]

    def to_dict(self):
        return dict([(c, getattr(self, c)) for c in self.columns()])


Model = declarative_base(cls=Base)


class User(Model):
    __tablename__ = "user"

    user_id = Column(Integer, primary_key=True, autoincrement=True)
    user_name = Column(String(45))

    def __init__(self):
        pass


class Repo(Model):
    __tablename__ = "repo"

    repo_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer)

    # 与 SmsRepo 中的 repo_name 重复，需要修改
    repo_name = Column(String(45))

    # repo 的类型：
    #   sms
    #   phy
    repo_type = Column(String(45))

    def __init__(self):
        pass


class SmsRepo(Model):
    __tablename__ = "sms_repo"

    # 仅该表中使用，其它表中使用 repo_id
    repo_id = Column(Integer, primary_key=True, autoincrement=False)
    user_id = Column(Integer, nullable=False)
    repo_name = Column(String(45))
    repo_location = Column(String(200))
    current_version_id = Column(String(20))
    repo_description = Column(Text())

    def __init__(self):
        pass

    def update_from_dict(self, repo_dict):
        if self.repo_id != repo_dict['repo_id']:
            return False
        # 只更新一部分内容，后面需要更改
        self.repo_location = repo_dict['repo_location']
        self.current_version_id = repo_dict['current_version_id']
        self.repo_description = repo_dict['repo_description']
        return True


class RepoVersion(Model):
    __tablename__ = 'repo_version'

    repo_id = Column(Integer, primary_key=True)
    version_id = Column(String(20), primary_key=True)
    version_name = Column(String(100))
    version_location = Column(String(200))
    head_line = Column(Text())
    collector_id = Column(Text())

    @staticmethod
    def create_from_dict(repo_version_dict):
        new_version = RepoVersion()
        new_version.repo_id = repo_version_dict['repo_id']
        new_version.version_id = repo_version_dict['version_id']
        new_version.version_name = repo_version_dict['version_name']
        new_version.version_location = repo_version_dict['version_location']
        new_version.head_line = repo_version_dict['head_line']
        new_version.collector_id = None
        return new_version


class RecordBase(object):
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
        self.record_string = line

        if not self.record_string.startswith("# "):
            """some line don't start with '# '

                exit

            just ignore it.
            """
            return

        start_pos = 2
        end_pos = line.find(':')
        self.record_type = line[start_pos:end_pos]

        start_pos = end_pos + 2
        end_pos = line.find(']', start_pos)
        if end_pos == -1:
            """some line is not like what we suppose it to be. Such as:

                # MSG:[02:50:38 22.10.2013] login:User nwp_sp@16239 with password from cma20n03
                readlists
                # MSG:[02:50:48 22.10.2013] logout:User nwp_sp@16239

            So we should check if the line starts with '#[...]'. If not, we don't parse it and just return.
            """
            return
        record_time_string = line[start_pos:end_pos]
        date_time = datetime.strptime(record_time_string, '%H:%M:%S %d.%m.%Y')
        self.record_date = date_time.date()
        self.record_time = date_time.time()

        start_pos = end_pos + 2
        end_pos = line.find(":", start_pos)
        if end_pos == -1:
            """
            some line is not like what we suppose it to be. Such as:

                # WAR:[21:05:13 25.9.2013] SCRIPT-NAME will return NULL, script is [/cma/g1/nwp_sp/SMSOUT/env_grib_v20/T639_ENV/gmf/12/upload/upload_003.sms]

            We need to check end_pos.
            """
            return
        self.record_command = line[start_pos:end_pos]

        if self.record_command in ('submitted', 'active', 'queued', 'complete', 'aborted', 'suspend'):
            start_pos = end_pos+1
            end_pos = line.find(' ', start_pos)
            if end_pos == -1:
                self.record_fullname = line[start_pos:].strip()
            else:
                self.record_fullname = line[start_pos:end_pos]
                self.record_additional_information = line[end_pos+1:]

        elif self.record_command == 'alter':
            start_pos = end_pos+1
            pos = line.find(' [', start_pos)
            if pos != -1:
                self.record_fullname = line[start_pos:pos]
                start_pos = pos + 2
                end_pos = line.find('] ', start_pos)
                if end_pos != -1:
                    self.record_additional_information = line[start_pos:end_pos]

        elif self.record_command == 'meter':
            if self.record_type != 'ERR':
                start_pos = end_pos + 1
                end_pos = line.find(' ', start_pos)
                self.record_fullname = line[start_pos:end_pos]
                start_pos = end_pos + 4
                self.record_additional_information = line[start_pos:]
            else:
                start_pos = end_pos + 1
                if line[start_pos] == "/":
                    # ERR:[12:52:00 19.5.2014] meter:/gmf_gsi_v1r5/T639/06/dasrefresh:WaitingMins: 55 out of range [0 - 40]
                    end_pos = line.find(' ', start_pos)
                    if end_pos != -1 and line[end_pos-1] == ":":
                        self.record_fullname = line[start_pos: end_pos-1]
                        self.record_additional_information = line[end_pos+1:]
                else:
                    # ERR:[03:41:58 10.7.2014] meter:WaitingMins for /grapes_meso_v4_0/cold/00/pre_data/obs_get/aob_get:the node was not found:
                    pass

        elif self.record_command in ['begin', 'autorepeat date']:
            start_pos = end_pos + 1
            end_pos = line.find(' ', start_pos)
            self.record_fullname = line[start_pos: end_pos]

        elif self.record_command == 'force' or self.record_command == 'force(recursively)':
            start_pos = end_pos + 1
            end_pos = line.find(' ', start_pos)
            self.record_fullname = line[start_pos:end_pos]
            if line[end_pos:end_pos+4] == " to ":
                start_pos = end_pos + 4
                end_pos = line.find(' ', start_pos)
                self.record_additional_information = line[start_pos:end_pos]

        elif self.record_command == 'delete':
            start_pos = end_pos + 1
            end_pos = line.find(' ', start_pos)
            self.record_fullname = line[start_pos:end_pos]
            start_pos = end_pos + 1
            end_pos = line.find(' ', start_pos)
            self.record_additional_information = line[start_pos:end_pos]

        elif self.record_command in ['set', 'clear']:
            start_pos = end_pos + 1
            end_pos = line.find(':', start_pos)
            if end_pos != -1:
                self.record_fullname = line[start_pos: end_pos]
                self.record_additional_information = line[end_pos+1:]

        # WAR:[23:37:08 16.8.2015] requeue:/gmf_grapes_v1423/grapes_global/12/post/postp_084 from aborted
        # MSG:[23:37:08 16.8.2015] requeue:user nwp@5986333:/gmf_grapes_v1423/grapes_global/12/post/postp_084
        elif self.record_command == 'requeue':
            start_pos = end_pos + 1
            if self.record_type == "WAR":
                end_pos = line.find(' ', start_pos)
                if end_pos != -1:
                    self.record_fullname = line[start_pos: end_pos]
                    start_pos = end_pos + 1
                    self.record_additional_information = line[start_pos:]
            elif self.record_type == "MSG":
                end_pos = line.find(':', start_pos)
                if end_pos != -1:
                    self.record_additional_information = line[start_pos: end_pos]
                    start_pos = end_pos + 1
                    self.record_fullname = line[start_pos:]


class RecordMixin(object):
    @declared_attr
    def __table_args__(cls):
        return (
            Index('{owner}_{repo}_date_time_index'.format(owner=cls.owner, repo=cls.repo), 'record_date', 'record_time'),
            Index('{owner}_{repo}_command_index'.format(owner=cls.owner, repo=cls.repo), 'record_command'),
            Index('{owner}_{repo}_fullname_index'.format(owner=cls.owner, repo=cls.repo), 'record_fullname'),
            Index('{owner}_{repo}_type_index'.format(owner=cls.owner, repo=cls.repo), 'record_type')
        )


class Record(RecordBase, Model):
    """
    SMS日志记录类的派生类，用于代表特定的表，见__tablename__。
    """
    __tablename__ = "record"

    owner = 'owner'
    repo = 'repo'

    def __init__(self):
        pass

    @classmethod
    def prepare(cls, owner, repo):
        """
        为 owner/repo 准备 Record 对象。当前需要修改 __tablename__ 为特定的表名。
        :param owner:
        :param repo:
        :return:
        """
        table_name = 'record.{owner}.{repo}'.format(owner=owner, repo=repo)
        cls.__table__.name = table_name
        cls.owner = owner
        cls.repo = repo

        cls.__table_args__ = (
            Index('{owner}_{repo}_date_time_index'.format(owner=cls.owner, repo=cls.repo), 'record_date', 'record_time'),
            Index('{owner}_{repo}_command_index'.format(owner=cls.owner, repo=cls.repo), 'record_command'),
            Index('{owner}_{repo}_fullname_index'.format(owner=cls.owner, repo=cls.repo), 'record_fullname'),
            Index('{owner}_{repo}_type_index'.format(owner=cls.owner, repo=cls.repo), 'record_type')
        )

    @classmethod
    def init(cls):
        cls.__table__.name = 'record'

        cls.owner = "owner"
        cls.repo = "repo"

        cls.__table_args__ = (
            Index('{owner}_{repo}_date_time_index'.format(owner=cls.owner, repo=cls.repo), 'record_date', 'record_time'),
            Index('{owner}_{repo}_command_index'.format(owner=cls.owner, repo=cls.repo), 'record_command'),
            Index('{owner}_{repo}_fullname_index'.format(owner=cls.owner, repo=cls.repo), 'record_fullname'),
            Index('{owner}_{repo}_type_index'.format(owner=cls.owner, repo=cls.repo), 'record_type')
        )
