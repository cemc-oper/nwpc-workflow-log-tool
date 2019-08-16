# coding: utf-8
from sqlalchemy import Column, Integer, String, Date, Time, Text, Index


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

    owner = "owner"
    repo = "repo"

    def __str__(self):
        return "<{class_name}(string='{record_string}')>".format(
            class_name=self.__class__.__name__, record_string=self.log_record.strip()
        )

    @classmethod
    def create_record_table(cls, owner, repo, session):
        cls.prepare(owner, repo)
        cls.__table__.create(bind=session.get_bind(), checkfirst=True)
        cls.init()

    @classmethod
    def prepare(cls, owner, repo):
        """
        为 owner/repo 准备 Record 对象。当前需要修改 __tablename__ 为特定的表名。
        :param owner:
        :param repo:
        :return:
        """
        table_name = "{table_prefix}.{owner}.{repo}".format(
            table_prefix=cls.__tablename__, owner=owner, repo=repo
        )
        cls.__table__.name = table_name
        cls.owner = owner
        cls.repo = repo

        cls.__create_index()

    @classmethod
    def init(cls):
        cls.__table__.name = cls.__tablename__

        cls.owner = "owner"
        cls.repo = "repo"

        cls.__create_index()

    @classmethod
    def __create_index(cls):
        cls.__table_args__ = (
            Index(
                "{owner}_{repo}_date_time_index".format(owner=cls.owner, repo=cls.repo),
                "date",
                "time",
            ),
            Index(
                "{owner}_{repo}_command_index".format(owner=cls.owner, repo=cls.repo),
                "command",
            ),
            Index(
                "{owner}_{repo}_fullname_index".format(owner=cls.owner, repo=cls.repo),
                "node_path",
            ),
            Index(
                "{owner}_{repo}_type_index".format(owner=cls.owner, repo=cls.repo),
                "log_type",
            ),
        )

    def parse(self, line):
        pass
