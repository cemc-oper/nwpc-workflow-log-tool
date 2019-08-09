# coding: utf-8
from sqlalchemy import Column, Integer, String, Text
from .model import Model


class Repo(Model):
    __tablename__ = "repo"

    repo_id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer)

    # 与 SmsRepo 中的 repo_name 重复，需要修改
    repo_name = Column(String(45))

    # repo 的类型：
    #   sms
    #   ecflow
    repo_type = Column(String(45))

    def __init__(self):
        pass


class RepoVersion(Model):
    __tablename__ = 'repo_version'

    repo_id = Column(Integer, primary_key=True)
    version_id = Column(String(20), primary_key=True)
    version_name = Column(String(100))
    version_location = Column(String(200))
    head_line = Column(Text())
    collector_id = Column(Text())

    @classmethod
    def create_from_dict(cls, repo_version_dict):
        new_version = cls()
        new_version.repo_id = repo_version_dict['repo_id']
        new_version.version_id = repo_version_dict['version_id']
        new_version.version_name = repo_version_dict['version_name']
        new_version.version_location = repo_version_dict['version_location']
        new_version.head_line = repo_version_dict['head_line']
        new_version.collector_id = None
        return new_version
